import os
import sys
import json
import csv
import sqlite3
import asyncio
import whisper
import logging
import numpy as np
from datetime import datetime
from telethon import TelegramClient, events
from telethon.tl.types import PeerChannel, MessageMediaDocument, MessageMediaPhoto
import imageio_ffmpeg
import soundfile as sf
from neo4j import GraphDatabase

def display_ascii_art():
    WHITE = "\033[97m"
    RESET = "\033[0m"
    
    art = r"""
___________________  _________
\__    ___/  _____/ /   _____/
  |    | /   \  ___ \_____  \ 
  |    | \    \_\  \/        \
  |____|  \______  /_______  /
                 \/        \/
    """
    
    print(WHITE + art + RESET)

display_ascii_art()

STATE_FILE = 'state.json'

def load_state():
    """Load state from file or create new state"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
    else:
        state = {
            'api_id': None,
            'api_hash': None,
            'phone': None,
            'channels': {},
            'channel_details': {},
            'scrape_media': False,
            'neo4j': {
                'url': None,
                'database': None,
                'password': None
            },
            'whisper_model': 'base'  # Default whisper model
        }
        save_state(state)
    
    # Ensure all required keys exist with default values
    if 'api_id' not in state:
        state['api_id'] = None
    if 'api_hash' not in state:
        state['api_hash'] = None
    if 'phone' not in state:
        state['phone'] = None
    if 'channels' not in state:
        state['channels'] = {}
    if 'channel_details' not in state:
        state['channel_details'] = {}
    if 'scrape_media' not in state:
        state['scrape_media'] = False
    if 'neo4j' not in state:
        state['neo4j'] = {
            'url': None,
            'database': None,
            'password': None
        }
    if 'whisper_model' not in state:
        state['whisper_model'] = 'base'
        save_state(state)
    
    return state

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def reset_state():
    """Reset state to default values"""
    state = {
        'api_id': None,
        'api_hash': None,
        'phone': None,
        'channels': {},
        'channel_details': {},
        'scrape_media': False,
        'neo4j': {
            'url': None,
            'database': None,
            'password': None
        },
        'whisper_model': 'base'
    }
    save_state(state)
    return state

state = load_state()

# Reset state if it's missing required keys
required_keys = {'api_id', 'api_hash', 'phone', 'channels', 'channel_details', 
                'scrape_media', 'neo4j', 'whisper_model'}
if not all(key in state for key in required_keys):
    print("Initializing state with default values...")
    state = reset_state()

if not state['api_id'] or not state['api_hash'] or not state['phone']:
    state['api_id'] = int(input("Enter your API ID: "))
    state['api_hash'] = input("Enter your API Hash: ")
    state['phone'] = input("Enter your phone number: ")
    save_state(state)

client = TelegramClient('session', state['api_id'], state['api_hash'])

async def save_message_to_db(message, channel_id, media_path=None):
    """Save message and its comments to the database"""
    # Create channel directory if it doesn't exist
    channel_dir = os.path.join(os.getcwd(), str(channel_id))
    os.makedirs(channel_dir, exist_ok=True)
    
    conn = sqlite3.connect(os.path.join(channel_dir, f'{channel_id}.db'))
    c = conn.cursor()
    
    # Create messages table if not exists
    c.execute(f'''CREATE TABLE IF NOT EXISTS messages
                  (id INTEGER PRIMARY KEY, message_id INTEGER, date TEXT, sender_id INTEGER, 
                   first_name TEXT, last_name TEXT, username TEXT, message TEXT, 
                   media_type TEXT, media_path TEXT, mime_type TEXT, reply_to INTEGER, transcript TEXT)''')
    
    # Create comments table if not exists
    c.execute(f'''CREATE TABLE IF NOT EXISTS comments
                  (id INTEGER PRIMARY KEY, comment_id INTEGER, message_id INTEGER, 
                   date TEXT, sender_id INTEGER, first_name TEXT, last_name TEXT, 
                   username TEXT, comment_text TEXT,
                   FOREIGN KEY(message_id) REFERENCES messages(message_id))''')
    
    # Get MIME type if it's a document
    mime_type = None
    if hasattr(message.media, 'document'):
        for attr in message.media.document.attributes:
            if hasattr(attr, 'mime_type'):
                mime_type = attr.mime_type
            elif hasattr(message.media.document, 'mime_type'):
                mime_type = message.media.document.mime_type
    
    # Get sender information safely
    sender_id = None
    first_name = None
    last_name = None
    username = None
    
    if message.sender:
        sender_id = message.sender_id
        if hasattr(message.sender, 'first_name'):
            first_name = message.sender.first_name
        if hasattr(message.sender, 'last_name'):
            last_name = message.sender.last_name
        if hasattr(message.sender, 'username'):
            username = message.sender.username
    
    # Save the message with ISO format date
    c.execute('''INSERT OR IGNORE INTO messages 
                 (message_id, date, sender_id, first_name, last_name, username, 
                  message, media_type, media_path, mime_type, reply_to, transcript)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
              (message.id, 
               message.date.isoformat(), 
               sender_id,
               first_name,
               last_name,
               username,
               message.message, 
               message.media.__class__.__name__ if message.media else None, 
               media_path,
               mime_type,
               message.reply_to_msg_id if message.reply_to else None,
               None))
    
    # If this is a comment (reply to another message), save it in comments table
    if message.reply_to:
        c.execute('''INSERT OR IGNORE INTO comments 
                     (comment_id, message_id, date, sender_id, first_name, last_name, 
                      username, comment_text)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (message.id,
                   message.reply_to_msg_id,
                   message.date.isoformat(),
                   sender_id,
                   first_name,
                   last_name,
                   username,
                   message.message))
    
    conn.commit()
    conn.close()

MAX_RETRIES = 5

async def download_media(channel_id, message):
    """Download media from a message"""
    if not message.media:
        return None
    
    try:
        # Create channel and media directories if they don't exist
        channel_dir = os.path.join(os.getcwd(), str(channel_id))
        media_dir = os.path.join(channel_dir, 'media')
        os.makedirs(media_dir, exist_ok=True)
        
        # Download the media
        path = await client.download_media(message, file=media_dir)
        if path:
            # Return just the filename, not the full path
            return os.path.basename(path)
        return None
    except Exception as e:
        print(f"Error downloading media: {e}")
        return None

async def rescrape_media(channel_id):
    channel_dir = os.path.join(os.getcwd(), channel_id)
    db_file = os.path.join(channel_dir, f'{channel_id}.db')
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute('SELECT message_id FROM messages WHERE media_type IS NOT NULL AND media_path IS NULL')
    rows = c.fetchall()
    conn.close()

    total_messages = len(rows)
    if total_messages == 0:
        print(f"No media files to reprocess for channel {channel_id}.")
        return

    for index, (message_id,) in enumerate(rows):
        try:
            entity = await client.get_entity(PeerChannel(int(channel_id)))
            message = await client.get_messages(entity, ids=message_id)
            media_path = await download_media(channel_id, message)
            if media_path:
                conn = sqlite3.connect(db_file)
                c = conn.cursor()
                c.execute('''UPDATE messages SET media_path = ? WHERE message_id = ?''', (media_path, message_id))
                conn.commit()
                conn.close()
            
            progress = (index + 1) / total_messages * 100
            sys.stdout.write(f"\rReprocessing media for channel {channel_id}: {progress:.2f}% complete")
            sys.stdout.flush()
        except Exception as e:
            print(f"Error reprocessing message {message_id}: {e}")
    print()

async def resolve_channel(channel_input):
    """Resolve channel name/ID to a proper channel entity"""
    try:
        # Handle channel IDs with or without -100 prefix
        if str(channel_input).startswith('-100'):
            peer_id = int(str(channel_input)[4:])  # Remove -100
        elif str(channel_input).startswith('-'):
            peer_id = int(str(channel_input)[1:])  # Remove -
        else:
            peer_id = int(channel_input) if str(channel_input).isdigit() else None

        # Try to get entity directly first
        if peer_id:
            try:
                return await client.get_entity(PeerChannel(peer_id))
            except ValueError:
                pass

        # If that fails, try getting entity by input string
        try:
            return await client.get_entity(channel_input)
        except ValueError:
            pass

        # If still not found, try searching dialogs
        try:
            async for dialog in client.iter_dialogs():
                dialog_id = str(dialog.id)
                if dialog_id.startswith('-100'):
                    dialog_id = dialog_id[4:]
                elif dialog_id.startswith('-'):
                    dialog_id = dialog_id[1:]
                
                if (dialog_id == str(peer_id if peer_id else channel_input) or 
                    dialog.name == channel_input):
                    return dialog.entity
        except Exception as e:
            print(f"Error searching dialogs: {e}")

        raise ValueError(f"Could not find channel: {channel_input}")
    except Exception as e:
        raise ValueError(f"Error resolving channel {channel_input}: {e}")

async def add_channel(channel_input):
    """Add a channel with proper resolution"""
    try:
        entity = await resolve_channel(channel_input)
        # Store channel ID without -100 prefix
        channel_id = str(entity.id)
        if channel_id.startswith('-100'):
            channel_id = channel_id[4:]
        elif channel_id.startswith('-'):
            channel_id = channel_id[1:]
            
        channel_title = entity.title if hasattr(entity, 'title') else str(entity.id)
        
        # Store both ID and title
        if 'channel_details' not in state:
            state['channel_details'] = {}
            
        state['channels'][channel_id] = 0
        state['channel_details'][channel_id] = {
            'title': channel_title,
            'username': entity.username if hasattr(entity, 'username') else None
        }
        save_state(state)
        print(f"Added channel: {channel_title} (ID: {channel_id})")
        return True
    except Exception as e:
        print(f"Failed to add channel {channel_input}: {e}")
        return False

async def scrape_channel(channel_id, offset_id=0):
    """Scrape a channel using its ID"""
    try:
        # Add -100 prefix if not present for proper resolution
        if not str(channel_id).startswith('-100'):
            channel_id = f"-100{channel_id}"
            
        entity = await resolve_channel(channel_id)
        if not entity:
            print(f"Could not resolve channel {channel_id}")
            return

        channel_title = entity.title if hasattr(entity, 'title') else str(entity.id)
        print(f"\nScraping channel: {channel_title}")
        
        try:
            total_messages = (await client.get_messages(entity, limit=1))[0].id
            processed_messages = 0
            
            async for message in client.iter_messages(entity, offset_id=offset_id, reverse=True):
                try:
                    sender = await message.get_sender()
                    # Use channel ID without -100 prefix for consistency with storage
                    await save_message_to_db(message, channel_id[4:] if channel_id.startswith('-100') else channel_id)
                    
                    if message.media and state['scrape_media']:
                        media_path = await download_media(channel_id[4:] if channel_id.startswith('-100') else channel_id,
                                                       message)
                        
                        if media_path:
                            conn = sqlite3.connect(os.path.join(os.getcwd(), 
                                                              channel_id[4:] if channel_id.startswith('-100') else channel_id, 
                                                              f'{channel_id[4:] if channel_id.startswith('-100') else channel_id}.db'))
                            c = conn.cursor()
                            c.execute('''UPDATE messages SET media_path = ? WHERE message_id = ?''', 
                                    (media_path, message.id))
                            conn.commit()
                            conn.close()
                    
                    last_message_id = message.id
                    processed_messages += 1

                    progress = (processed_messages / total_messages) * 100
                    sys.stdout.write(f"\rScraping channel: {channel_title} - Progress: {progress:.2f}%")
                    sys.stdout.flush()

                    state['channels'][channel_id[4:] if channel_id.startswith('-100') else channel_id] = last_message_id
                    save_state(state)
                except Exception as e:
                    print(f"\nError processing message {message.id}: {e}")
            print()
        except Exception as e:
            print(f"\nError scraping messages: {e}")
    except ValueError as e:
        print(f"Error with channel {channel_id}: {e}")

async def continuous_scraping():
    global continuous_scraping_active
    continuous_scraping_active = True

    print("\nStarting continuous scraping mode...")
    print("Press Ctrl+C to stop scraping and return to menu")
    print("=" * 50)

    try:
        while continuous_scraping_active:
            for channel in state['channels']:
                if not continuous_scraping_active:
                    break
                print(f"\nChecking for new messages in channel: {channel}")
                try:
                    await scrape_channel(channel, state['channels'][channel])
                    print(f"New messages or media scraped from channel: {channel}")
                except Exception as e:
                    print(f"Error scraping channel {channel}: {e}")
                    continue
            
            print("\nWaiting 60 seconds before next check... (Press Ctrl+C to stop)")
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                raise
    except (asyncio.CancelledError, KeyboardInterrupt):
        continuous_scraping_active = False
        print("\nStopping continuous scraping...")
        print("Returning to menu...")

async def export_data():
    for channel in state['channels']:
        print(f"\nExporting data for channel: {channel}")
        await export_to_csv(channel)
        await export_to_json(channel)
        print(f"Exported data for {channel} to CSV and JSON files")

async def export_to_csv(channel_id):
    """Export messages and comments to CSV files"""
    try:
        channel_dir = os.path.join(os.getcwd(), str(channel_id))
        db_file = os.path.join(channel_dir, f'{channel_id}.db')
        
        if not os.path.exists(db_file):
            print(f"No database file found for channel {channel_id}")
            return
        
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        
        try:
            # Export messages
            output_file = os.path.join(channel_dir, f'{channel_id}_messages.csv')
            c.execute('''SELECT message_id, date, message, media_type, media_path, mime_type, transcript
                        FROM messages''')
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Message ID', 'Date', 'Message', 'Media Type', 'Media Path', 'MIME Type', 'Transcript'])
                writer.writerows(c.fetchall())
            
            print(f"Messages exported to {output_file}")
            
            # Export comments
            output_file = os.path.join(channel_dir, f'{channel_id}_comments.csv')
            c.execute('SELECT * FROM comments')
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([description[0] for description in c.description])
                writer.writerows(c.fetchall())
            
            print(f"Comments exported to {output_file}")
        except Exception as e:
            print(f"Error writing CSV file: {str(e)}")
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Error exporting to CSV: {str(e)}")

async def export_to_json(channel_id):
    channel_dir = os.path.join(os.getcwd(), channel_id)
    db_file = os.path.join(channel_dir, f'{channel_id}.db')
    json_file = os.path.join(channel_dir, f'{channel_id}.json')
    
    if not os.path.exists(db_file):
        print(f"No database file found for channel {channel_id}")
        return
        
    try:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        
        # Get messages
        c.execute('''SELECT * FROM messages''')
        columns = [description[0] for description in c.description]
        messages = [dict(zip(columns, row)) for row in c.fetchall()]
        
        # Get comments
        c.execute('''SELECT * FROM comments''')
        comment_columns = [description[0] for description in c.description]
        comments = [dict(zip(comment_columns, row)) for row in c.fetchall()]
        
        # Combine data
        data = {
            'channel': channel_id,
            'messages': messages,
            'comments': comments
        }
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        conn.close()
        print(f"JSON export completed for {channel_id}")
    except Exception as e:
        print(f"Error exporting to JSON for channel {channel_id}: {e}")

async def view_channels():
    """View detailed information about saved channels including message and media stats"""
    print("\nSaved Channels Statistics:")
    if not state['channels']:
        print("No channels saved.")
        return

    for channel_id in state['channels']:
        channel_details = state.get('channel_details', {}).get(channel_id, {})
        channel_title = channel_details.get('title', channel_id)
        username = channel_details.get('username', 'N/A')
        
        # Get database statistics
        channel_dir = os.path.join(os.getcwd(), str(channel_id))
        db_file = os.path.join(channel_dir, f'{channel_id}.db')
        
        if os.path.exists(db_file):
            conn = sqlite3.connect(db_file)
            c = conn.cursor()
            
            # Get message count
            c.execute('SELECT COUNT(*) FROM messages')
            message_count = c.fetchone()[0]
            
            # Get media count
            c.execute('SELECT COUNT(*) FROM messages WHERE media_type IS NOT NULL AND media_type != ""')
            media_count = c.fetchone()[0]
            
            # Get comment count
            c.execute('SELECT COUNT(*) FROM comments')
            comment_count = c.fetchone()[0]
            
            # Get date range
            c.execute('SELECT MIN(date), MAX(date) FROM messages')
            date_range = c.fetchone()
            first_message = date_range[0] if date_range[0] else 'N/A'
            last_message = date_range[1] if date_range[1] else 'N/A'
            
            conn.close()
            
            print(f"\n{channel_title} (@{username})")
            print(f"Channel ID: {channel_id}")
            print(f"Messages: {message_count}")
            print(f"Comments: {comment_count}")
            print(f"Media items: {media_count}")
            print(f"First message: {first_message}")
            print(f"Last message: {last_message}")
            print("-" * 50)
        else:
            print(f"\n{channel_title} (@{username})")
            print(f"Channel ID: {channel_id}")
            print("No data scraped yet")
            print("-" * 50)

async def list_channels():
    """List all available channels with their details"""
    try:
        channels = []
        async for dialog in client.iter_dialogs():
            if dialog.is_channel:
                channel_id = str(dialog.id)
                if channel_id.startswith('-100'):
                    channel_id = channel_id[4:]
                elif channel_id.startswith('-'):
                    channel_id = channel_id[1:]
                
                channels.append({
                    'id': channel_id,
                    'name': dialog.name,
                    'entity': dialog.entity
                })
        
        if not channels:
            print("\nNo channels found.")
            return []
            
        print("\nAvailable channels:")
        for idx, channel in enumerate(channels, 1):
            print(f"{idx}. {channel['name']} (ID: {channel['id']})")
        
        return channels
    except Exception as e:
        print(f"Error listing channels: {e}")
        return []

async def list_saved_channels():
    """List saved channels with indices"""
    channels = []
    print("\nSaved channels:")
    for channel_id in state['channels']:
        channel_details = state.get('channel_details', {}).get(channel_id, {})
        channels.append({
            'id': channel_id,
            'title': channel_details.get('title', channel_id),
            'username': channel_details.get('username')
        })
    
    if not channels:
        print("No channels saved.")
        return []
        
    for idx, channel in enumerate(channels, 1):
        username_str = f" (@{channel['username']})" if channel['username'] else ""
        print(f"{idx}. {channel['title']}{username_str} (ID: {channel['id']})")
    
    return channels

async def manage_channels():
    """Channel management submenu"""
    while True:
        print("\nChannel Management Menu:")
        print("[L] List available channels")
        print("[A] Add channel")
        print("[V] View saved channels stats")
        print("[S] Start scraping")
        print("[E] Export data")
        print("[N] Upload to Neo4j")
        print("[R] Remove channel")
        print("[T] Transcribe media")
        print("[B] Back to main menu")
        
        choice = input("\nEnter your choice: ").upper()
        
        if choice == 'L':
            channels = await list_channels()
            if channels:
                print("\nEnter a number to add that channel, or any other input to return to menu.")
                try:
                    idx = int(input("Channel number to add (or other input to cancel): "))
                    if 1 <= idx <= len(channels):
                        channel = channels[idx - 1]
                        await add_channel(channel['id'])
                except ValueError:
                    continue
                
        elif choice == 'A':
            channel_input = input("Enter channel ID, username, or title: ")
            await add_channel(channel_input)
            
        elif choice == 'V':
            await view_channels()
            
        elif choice == 'S':
            print("\nStarting scraping process...")
            await continuous_scraping()
            
        elif choice == 'E':
            await export_data()
            
        elif choice == 'N':
            print("\nConnecting to Neo4j...")
            if await setup_neo4j_connection():
                # Get list of channels
                print("\nSaved channels:")
                channels = list(state.get('channels', {}).keys())
                for i, channel in enumerate(channels, 1):
                    title = state.get('channel_details', {}).get(channel, {}).get('title', 'Unknown')
                    print(f"{i}. {title} (ID: {channel})")
                
                # Get channel selection
                choice = input("\nEnter a number to upload that channel, or any other input to return to menu.\nChannel number to process (or other input to cancel): ")
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(channels):
                        await upload_to_neo4j(channels[idx])
                except ValueError:
                    print("Returning to menu...")
            
        elif choice == 'R':
            channels = await list_saved_channels()
            if channels:
                print("\nEnter a number to remove that channel, or any other input to return to menu.")
                try:
                    idx = int(input("Channel number to remove (or other input to cancel): "))
                    if 1 <= idx <= len(channels):
                        channel = channels[idx - 1]
                        channel_id = channel['id']
                        
                        # Remove from state
                        del state['channels'][channel_id]
                        if 'channel_details' in state and channel_id in state['channel_details']:
                            del state['channel_details'][channel_id]
                        save_state(state)
                        
                        print(f"Removed channel: {channel['title']} (ID: {channel_id})")
                except ValueError:
                    continue
                
        elif choice == 'T':
            channels = await list_saved_channels()
            if channels:
                print("\nEnter a number to transcribe media for that channel, or any other input to return to menu.")
                try:
                    idx = int(input("Channel number to process (or other input to cancel): "))
                    if 1 <= idx <= len(channels):
                        channel = channels[idx - 1]
                        await transcribe_media(channel['id'])
                except ValueError:
                    continue
                
        elif choice == 'B':
            break
            
        else:
            print("Invalid choice. Please try again.")

async def main_menu():
    """Main menu of the application"""
    while True:
        print("\nMain Menu:")
        print("[C] Channel Management")
        print("[M] Toggle Media Scraping (currently {})".format(
            "enabled" if state['scrape_media'] else "disabled"))
        print("[W] Change Whisper Model (currently {})".format(state['whisper_model']))
        print("[R] Reset Menu")
        print("[Q] Quit")
        
        choice = input("\nEnter your choice: ").upper()
        
        if choice == 'C':
            await manage_channels()
        elif choice == 'M':
            state['scrape_media'] = not state['scrape_media']
            save_state(state)
            print(f"Media scraping {'enabled' if state['scrape_media'] else 'disabled'}.")
        elif choice == 'W':
            print("\nAvailable Whisper Models:")
            for model, description in WHISPER_MODELS.items():
                print(f"- {model}: {description}")
            
            model = input("\nEnter model name (or press Enter to keep current): ").lower()
            if model in WHISPER_MODELS:
                state['whisper_model'] = model
                save_state(state)
                print(f"Whisper model changed to: {model}")
            elif model:
                print("Invalid model name. Keeping current model.")
        elif choice == 'R':
            await reset_menu()
        elif choice == 'Q':
            print("\nExiting program...")
            sys.exit()
        else:
            print("Invalid choice. Please try again.")

async def upload_to_neo4j(channel_id):
    """Upload channel data to Neo4j"""
    try:
        neo4j_config = state.get('neo4j', {})
        if not neo4j_config or not neo4j_config.get('url') or not neo4j_config.get('password'):
            print("Neo4j connection details not found in state.json")
            return

        channel_dir = os.path.join(os.getcwd(), str(channel_id))
        db_file = os.path.join(channel_dir, f'{channel_id}.db')
        
        if not os.path.exists(db_file):
            print(f"No database file found for channel {channel_id}")
            return
            
        # Connect to Neo4j
        driver = GraphDatabase.driver(
            neo4j_config['url'],
            auth=("neo4j", neo4j_config['password'])
        )
        
        # Connect to SQLite
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        
        with driver.session() as session:
            # Create full-text search indexes if they don't exist
            try:
                # Create index for Message nodes
                session.run("""
                    CREATE FULLTEXT INDEX message_content IF NOT EXISTS
                    FOR (n:Message)
                    ON EACH [n.message]
                """)
                
                # Create index for Comment nodes
                session.run("""
                    CREATE FULLTEXT INDEX comment_content IF NOT EXISTS
                    FOR (n:Comment)
                    ON EACH [n.text]
                """)

                # Create index for Transcript nodes
                session.run("""
                    CREATE FULLTEXT INDEX transcript_content IF NOT EXISTS
                    FOR (n:Transcript)
                    ON EACH [n.transcript]
                """)
            except Exception as e:
                print(f"Warning: Could not create full-text indexes: {str(e)}")
            
            # Get channel name from state
            channel_name = state.get('channel_details', {}).get(str(channel_id), {}).get('title', str(channel_id))
            
            # Create Channel node with name
            session.run("""
                MERGE (c:Channel {id: $channel_id})
                SET c.channel_id = $channel_id,
                    c.name = $channel_name
            """, channel_id=str(channel_id), channel_name=channel_name)
            
            # Get and create Message nodes with Media and Transcript relationships
            c.execute('''SELECT message_id, date, message, media_type, media_path, mime_type, transcript, reply_to,
                               sender_id, first_name, last_name, username 
                        FROM messages''')
            messages = c.fetchall()
            
            for msg in messages:
                (msg_id, date, message_text, media_type, media_path, mime_type, transcript, reply_to,
                 sender_id, first_name, last_name, username) = msg
                
                # Skip messages without text content (like channel creation messages)
                if not message_text and not media_path:
                    continue

                # Create display name for sender
                sender_name = ' '.join(filter(None, [first_name, last_name])) if first_name or last_name else username or str(sender_id)
                
                # Create preview text (truncate at 50 chars)
                preview_text = (message_text[:47] + "...") if message_text and len(message_text) > 50 else message_text
                
                # Create Message node with sender info and preview
                session.run("""
                    MERGE (m:Message {id: $msg_id})
                    SET m.date = $date,
                        m.message = $message_text,
                        m.preview = $preview_text,
                        m.reply_to = $reply_to,
                        m.sender_name = $sender_name,
                        m.username = $username
                    WITH m
                    MATCH (c:Channel {id: $channel_id})
                    MERGE (c)-[:HAS_MESSAGE]->(m)
                """, msg_id=str(msg_id), date=date, message_text=message_text, preview_text=preview_text,
                     reply_to=str(reply_to) if reply_to else None, channel_id=str(channel_id),
                     sender_name=sender_name, username=username)
                
                # If there's media, create Media node with better labels
                if media_path:
                    # Create hash from media path for unique ID
                    import hashlib
                    media_hash = hashlib.md5(media_path.encode()).hexdigest()
                    
                    # Get absolute path for file:/// URL
                    abs_path = os.path.abspath(os.path.join(channel_dir, 'media', media_path))
                    file_url = f"file:///{abs_path.replace(os.sep, '/')}"
                    
                    # Get filename for label
                    filename = os.path.basename(media_path)
                    
                    # Create Media node with appropriate properties
                    media_props = {
                        'id': media_hash,
                        'type': media_type,
                        'mime_type': mime_type,
                        'path': media_path,
                        'filename': filename
                    }
                    
                    # Add thumbnail for images
                    if mime_type and mime_type.startswith('image/'):
                        media_props['thumbnail'] = file_url
                    
                    session.run("""
                        MERGE (media:Media {id: $id})
                        SET media += $props
                        WITH media
                        MATCH (m:Message {id: $msg_id})
                        MERGE (m)-[:HAS_MEDIA]->(media)
                    """, id=media_hash, props=media_props, msg_id=str(msg_id))
                    
                    # If there's a transcript, create Transcript node with preview
                    if transcript:
                        transcript_hash = hashlib.md5(f"{media_hash}_transcript".encode()).hexdigest()
                        transcript_preview = (transcript[:47] + "...") if len(transcript) > 50 else transcript
                        
                        session.run("""
                            MERGE (t:Transcript {id: $id})
                            SET t.transcript = $transcript,
                                t.preview = $preview
                            WITH t
                            MATCH (media:Media {id: $media_id})
                            MERGE (media)-[:HAS_TRANSCRIPT]->(t)
                        """, id=transcript_hash, transcript=transcript, preview=transcript_preview, media_id=media_hash)
            
            # Get and create Comment nodes with sender info and preview
            c.execute('''SELECT comment_id, message_id, comment_text, sender_id, first_name, last_name, username 
                        FROM comments''')
            comments = c.fetchall()
            
            for comment in comments:
                comment_id, message_id, comment_text, sender_id, first_name, last_name, username = comment
                
                # Create display name for commenter
                sender_name = ' '.join(filter(None, [first_name, last_name])) if first_name or last_name else username or str(sender_id)
                
                # Create preview text
                preview_text = (comment_text[:47] + "...") if comment_text and len(comment_text) > 50 else comment_text
                
                session.run("""
                    MERGE (c:Comment {id: $comment_id})
                    SET c.text = $comment_text,
                        c.preview = $preview_text,
                        c.sender_name = $sender_name,
                        c.username = $username
                    WITH c
                    MATCH (m:Message {id: $message_id})
                    MERGE (m)-[:HAS_COMMENT]->(c)
                """, comment_id=str(comment_id), comment_text=comment_text, preview_text=preview_text,
                     sender_name=sender_name, username=username, message_id=str(message_id))
        
        print(f"Successfully uploaded channel {channel_id} to Neo4j")
        driver.close()
        conn.close()
        
    except Exception as e:
        print(f"Error uploading to Neo4j: {str(e)}")

async def setup_neo4j_connection():
    """Setup Neo4j connection details"""
    try:
        # Use existing credentials if available
        neo4j_config = state.get('neo4j', {})
        if neo4j_config.get('url') and neo4j_config.get('password'):
            url = neo4j_config['url']
            password = neo4j_config['password']
            database = neo4j_config.get('database', 'neo4j')
        else:
            connection_type = input("Connect to Local [L] or Remote [R] Neo4j database? ").lower()
            
            if connection_type == 'l':
                url = "bolt://localhost:7687"
            else:
                url = input("Enter Neo4j URL (e.g., bolt://example.com:7687): ")
            
            database = input("Enter database name (press Enter for 'neo4j'): ").strip()
            if not database:
                database = "neo4j"
                
            password = input("Enter database password: ")
        
        # Test connection
        driver = GraphDatabase.driver(url, auth=("neo4j", password))
        with driver.session() as session:
            # Try a simple query to verify connection
            session.run("RETURN 1")
        driver.close()
        
        # Save to state if new connection
        if not neo4j_config:
            state['neo4j'] = {
                'type': 'local' if url == "bolt://localhost:7687" else 'remote',
                'url': url,
                'database': database,
                'password': password
            }
            save_state(state)
        
        print("Successfully connected to Neo4j database!")
        return True
        
    except Exception as e:
        print(f"Failed to connect to Neo4j: {str(e)}")
        return False

async def get_media_files(channel_id):
    """Get all media files for a channel that haven't been transcribed"""
    channel_dir = os.path.join(os.getcwd(), str(channel_id))
    media_dir = os.path.join(channel_dir, 'media')
    
    print(f"\nChecking media directory: {media_dir}")
    if not os.path.exists(media_dir):
        print("Media directory not found")
        return []
    
    # Connect to database to check for existing transcripts
    db_file = os.path.join(channel_dir, f'{channel_id}.db')
    print(f"Checking database: {db_file}")
    
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    
    # Add transcript and mime_type columns if they don't exist
    c.execute('''
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='messages'
    ''')
    if c.fetchone():
        c.execute('''
            SELECT sql FROM sqlite_master 
            WHERE type='table' AND name='messages'
        ''')
        create_stmt = c.fetchone()[0]
        if 'transcript' not in create_stmt:
            print("Adding transcript column to messages table")
            c.execute('ALTER TABLE messages ADD COLUMN transcript TEXT')
            conn.commit()
        if 'mime_type' not in create_stmt:
            print("Adding mime_type column to messages table")
            c.execute('ALTER TABLE messages ADD COLUMN mime_type TEXT')
            conn.commit()
    
    # Get files that need transcription
    print("\nChecking for media files that need transcription...")
    
    # First, check what media types and MIME types we have
    c.execute('''
        SELECT DISTINCT media_type, mime_type
        FROM messages 
        WHERE media_type IS NOT NULL
    ''')
    types = c.fetchall()
    print("Found media types:")
    for media_type, mime_type in types:
        print(f"- {media_type} (MIME: {mime_type})")
    
    # Get files that need transcription
    c.execute('''
        SELECT media_path, media_type, mime_type
        FROM messages 
        WHERE media_type IS NOT NULL
        AND media_path IS NOT NULL
    ''')
    all_media = c.fetchall()
    print(f"Total media files in database: {len(all_media)}")
    
    # List all files in media directory
    media_files = set(os.listdir(media_dir))
    print(f"Files in media directory: {len(media_files)}")
    for file in media_files:
        print(f"Found file: {file}")
    
    # Query for audio/video content based on media_type or mime_type
    c.execute('''
        SELECT media_path, media_type, mime_type
        FROM messages 
        WHERE (
            media_type LIKE '%video%' 
            OR media_type LIKE '%audio%' 
            OR media_type LIKE '%voice%' 
            OR media_type LIKE '%round%'
            OR media_type = 'MessageMediaDocument'
            OR mime_type LIKE 'audio/%'
            OR mime_type LIKE 'video/%'
        )
        AND media_path IS NOT NULL
        AND (transcript IS NULL OR transcript = '')
    ''')
    
    files_to_process = []
    for row in c.fetchall():
        media_path, media_type, mime_type = row
        # Use just the filename from media_path
        media_filename = os.path.basename(media_path)
        if media_filename in media_files:
            files_to_process.append(media_filename)
            print(f"Found file to transcribe: {media_filename} (Type: {media_type}, MIME: {mime_type})")
        else:
            print(f"File not found in media directory: {media_filename}")
    
    conn.close()
    
    return files_to_process

def extract_audio(video_path, output_path):
    """Extract audio from video using imageio-ffmpeg"""
    import subprocess
    
    ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
    
    # Build the ffmpeg command
    cmd = [
        ffmpeg_path,
        '-i', video_path,  # Input
        '-vn',  # No video
        '-acodec', 'pcm_s16le',  # Audio codec
        '-ar', '16000',  # Sample rate
        '-ac', '1',  # Mono
        '-y',  # Overwrite output
        output_path
    ]
    
    # Run the command
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    
    if process.returncode != 0:
        error_msg = stderr.decode() if stderr else "Unknown error"
        raise Exception(f"FFmpeg failed with return code {process.returncode}: {error_msg}")
    
    if not os.path.exists(output_path):
        raise Exception(f"FFmpeg did not create output file: {output_path}")
    
    return output_path

async def transcribe_media(channel_id):
    try:
        print(f"\nInitializing Whisper model ({state['whisper_model']})...")
        model = whisper.load_model(state['whisper_model'])
        
        files = await get_media_files(channel_id)
        if not files:
            print("No new media files to transcribe.")
            return
        
        print(f"Found {len(files)} files to transcribe.")
        channel_dir = os.path.join(os.getcwd(), str(channel_id))
        media_dir = os.path.join(channel_dir, 'media')
        temp_dir = os.path.join(channel_dir, 'temp')
        os.makedirs(temp_dir, exist_ok=True)
        
        # Connect to database
        db_file = os.path.join(channel_dir, f'{channel_id}.db')
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        
        for i, media_filename in enumerate(files, 1):
            try:
                video_path = os.path.join(media_dir, media_filename)
                print(f"\nProcessing file {i}/{len(files)}: {media_filename}")
                
                # Extra path verification
                if not os.path.exists(video_path):
                    print(f"Video file does not exist: {video_path}")
                    continue
                    
                if not os.path.isfile(video_path):
                    print(f"Not a file: {video_path}")
                    continue
                
                try:
                    # Extract audio to temp WAV file
                    audio_filename = os.path.splitext(media_filename)[0] + '.wav'
                    audio_path = os.path.join(temp_dir, audio_filename)
                    print(f"Extracting audio...")
                    
                    extract_audio(video_path, audio_path)
                    
                    # Load and transcribe the audio
                    print("Loading audio...")
                    audio_data, sample_rate = sf.read(audio_path)
                    audio_data = audio_data.astype(np.float32)
                    
                    if sample_rate != 16000:
                        audio_data = whisper.pad_or_trim(audio_data)
                    
                    print("Transcribing audio...")
                    result = model.transcribe(audio_data, fp16=False)
                    transcript = result["text"].strip()
                    
                    # Update database with transcript
                    c.execute('''
                        UPDATE messages 
                        SET transcript = ? 
                        WHERE media_path LIKE ?
                    ''', (transcript, f'%{media_filename}'))
                    conn.commit()
                    
                    print(f"Transcription successful: {len(transcript)} characters")
                    print(f"Transcript preview: {transcript[:200]}..." if len(transcript) > 200 else f"Transcript: {transcript}")
                    
                    # Clean up temp file
                    try:
                        os.remove(audio_path)
                    except Exception as e:
                        print(f"Warning: Could not remove temp file {audio_path}: {e}")
                except Exception as e:
                    print(f"Error during transcription:")
                    print(f"Error type: {type(e).__name__}")
                    print(f"Error message: {str(e)}")
                    print(f"Full error: {sys.exc_info()}")
                    continue
                
            except Exception as e:
                print(f"Error processing {media_filename}:")
                print(f"Error type: {type(e).__name__}")
                print(f"Error message: {str(e)}")
                print(f"Full error: {sys.exc_info()}")
                continue
        
        conn.close()
        print("\nTranscription complete!")
        
        # Clean up temp directory
        try:
            import shutil
            shutil.rmtree(temp_dir)
        except Exception as e:
            print(f"Warning: Could not remove temp directory {temp_dir}: {e}")
        
    except Exception as e:
        print(f"Error during transcription process:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")

WHISPER_MODELS = {
    'tiny': 'Fastest, lowest accuracy',
    'base': 'Fast, decent accuracy',
    'small': 'Balanced speed and accuracy',
    'medium': 'Good accuracy, slower',
    'large': 'Best accuracy, slowest'
}

def reset_telegram_account():
    """Reset Telegram account details"""
    global state
    # Remove session file
    if os.path.exists('session'):
        os.remove('session')
    if os.path.exists('session.session'):
        os.remove('session.session')
    
    # Clear Telegram-related state
    state['api_id'] = None
    state['api_hash'] = None
    state['phone'] = None
    state['channels'] = {}
    state['channel_details'] = {}
    save_state(state)
    print("\nTelegram account details have been reset.")
    print("You will need to re-enter your API credentials on next startup.")

def reset_neo4j_connection():
    """Reset Neo4j connection details"""
    global state
    state['neo4j'] = {
        'url': None,
        'database': None,
        'password': None
    }
    save_state(state)
    print("\nNeo4j connection details have been reset.")

def wipe_local_data():
    """Wipe all local databases and channel folders"""
    global state
    
    # Get list of channel IDs/names from state
    channel_ids = list(state['channels'].keys())
    
    if not channel_ids:
        print("\nNo local data to wipe.")
        return
    
    print("\nWARNING: This will permanently delete all local databases and channel folders.")
    print("The following channels will be affected:")
    for channel_id in channel_ids:
        channel_name = state['channel_details'].get(channel_id, {}).get('title', channel_id)
        print(f"- {channel_name} (ID: {channel_id})")
    
    confirmation = input("\nType 'DELETE' to confirm deletion: ")
    if confirmation != 'DELETE':
        print("Operation cancelled.")
        return
    
    # Delete channel folders and their contents
    for channel_id in channel_ids:
        channel_dir = os.path.join(os.getcwd(), str(channel_id))
        if os.path.exists(channel_dir):
            try:
                import shutil
                shutil.rmtree(channel_dir)
                print(f"Deleted folder: {channel_dir}")
            except Exception as e:
                print(f"Error deleting {channel_dir}: {e}")
    
    print("\nLocal data has been wiped.")

async def reset_menu():
    """Reset menu for various reset operations"""
    while True:
        print("\nReset Menu:")
        print("[T] Reset Telegram Account")
        print("[N] Reset Neo4j Connection")
        print("[W] Wipe Local Data")
        print("[B] Back to Main Menu")
        
        choice = input("\nEnter your choice: ").upper()
        
        if choice == 'T':
            print("\nThis will remove your Telegram account details and channel list.")
            print("Local databases and files will remain intact.")
            confirmation = input("Are you sure? (y/N): ")
            if confirmation.lower() == 'y':
                reset_telegram_account()
        
        elif choice == 'N':
            print("\nThis will remove your Neo4j connection details.")
            confirmation = input("Are you sure? (y/N): ")
            if confirmation.lower() == 'y':
                reset_neo4j_connection()
        
        elif choice == 'W':
            wipe_local_data()
        
        elif choice == 'B':
            break
        
        else:
            print("Invalid choice. Please try again.")

async def main():
    await client.start()
    try:
        await main_menu()
    except KeyboardInterrupt:
        print("\nGracefully shutting down...")
        # Ensure continuous scraping is stopped
        global continuous_scraping_active
        continuous_scraping_active = False

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted. Exiting...")
        sys.exit()
