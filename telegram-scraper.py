import os
import sqlite3
import json
import csv
import asyncio
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, User, PeerChannel
from telethon.errors import FloodWaitError, RPCError
import aiohttp
import sys
from neo4j import GraphDatabase
import whisper
import ffmpeg

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
    conn = sqlite3.connect(os.path.join(os.getcwd(), str(channel_id), f'{channel_id}.db'))
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
    
    # Save the message with ISO format date
    c.execute('''INSERT OR IGNORE INTO messages 
                 (message_id, date, sender_id, first_name, last_name, username, 
                  message, media_type, media_path, mime_type, reply_to, transcript)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
              (message.id, 
               message.date.isoformat(), 
               message.sender_id,
               message.sender.first_name if message.sender else None,
               message.sender.last_name if message.sender else None,
               message.sender.username if message.sender else None,
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
                   message.sender_id,
                   message.sender.first_name if message.sender else None,
                   message.sender.last_name if message.sender else None,
                   message.sender.username if message.sender else None,
                   message.message))
    
    conn.commit()
    conn.close()

MAX_RETRIES = 5

async def download_media(channel, message):
    if not message.media or not state['scrape_media']:
        return None

    channel_dir = os.path.join(os.getcwd(), channel)
    media_folder = os.path.join(channel_dir, 'media')
    os.makedirs(media_folder, exist_ok=True)    
    media_file_name = None
    if isinstance(message.media, MessageMediaPhoto):
        media_file_name = message.file.name or f"{message.id}.jpg"
    elif isinstance(message.media, MessageMediaDocument):
        media_file_name = message.file.name or f"{message.id}.{message.file.ext if message.file.ext else 'bin'}"
    
    if not media_file_name:
        print(f"Unable to determine file name for message {message.id}. Skipping download.")
        return None
    
    media_path = os.path.join(media_folder, media_file_name)
    
    if os.path.exists(media_path):
        print(f"Media file already exists: {media_path}")
        return media_path

    retries = 0
    while retries < MAX_RETRIES:
        try:
            if isinstance(message.media, MessageMediaPhoto):
                media_path = await message.download_media(file=media_folder)
            elif isinstance(message.media, MessageMediaDocument):
                media_path = await message.download_media(file=media_folder)
            if media_path:
                print(f"Successfully downloaded media to: {media_path}")
            break
        except (TimeoutError, aiohttp.ClientError, RPCError) as e:
            retries += 1
            print(f"Retrying download for message {message.id}. Attempt {retries}...")
            await asyncio.sleep(2 ** retries)
    return media_path

async def rescrape_media(channel):
    channel_dir = os.path.join(os.getcwd(), channel)
    db_file = os.path.join(channel_dir, f'{channel}.db')
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute('SELECT message_id FROM messages WHERE media_type IS NOT NULL AND media_path IS NULL')
    rows = c.fetchall()
    conn.close()

    total_messages = len(rows)
    if total_messages == 0:
        print(f"No media files to reprocess for channel {channel}.")
        return

    for index, (message_id,) in enumerate(rows):
        try:
            entity = await client.get_entity(PeerChannel(int(channel)))
            message = await client.get_messages(entity, ids=message_id)
            media_path = await download_media(channel, message)
            if media_path:
                conn = sqlite3.connect(db_file)
                c = conn.cursor()
                c.execute('''UPDATE messages SET media_path = ? WHERE message_id = ?''', (media_path, message_id))
                conn.commit()
                conn.close()
            
            progress = (index + 1) / total_messages * 100
            sys.stdout.write(f"\rReprocessing media for channel {channel}: {progress:.2f}% complete")
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

    try:
        while continuous_scraping_active:
            for channel in state['channels']:
                print(f"\nChecking for new messages in channel: {channel}")
                await scrape_channel(channel, state['channels'][channel])
                print(f"New messages or media scraped from channel: {channel}")
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        print("Continuous scraping stopped.")
        continuous_scraping_active = False

async def export_data():
    for channel in state['channels']:
        print(f"\nExporting data for channel: {channel}")
        await export_to_csv(channel)
        await export_to_json(channel)
        print(f"Exported data for {channel} to CSV and JSON files")

async def export_to_csv(channel):
    channel_dir = os.path.join(os.getcwd(), channel)
    db_file = os.path.join(channel_dir, f'{channel}.db')
    csv_file = os.path.join(channel_dir, f'{channel}.csv')
    
    if not os.path.exists(db_file):
        print(f"No database file found for channel {channel}")
        return
        
    try:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        
        # Export messages
        c.execute('SELECT * FROM messages')
        rows = c.fetchall()
        
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([description[0] for description in c.description])
            writer.writerows(rows)
            
        # Export comments to a separate CSV
        comments_csv_file = os.path.join(channel_dir, f'{channel}_comments.csv')
        c.execute('SELECT * FROM comments')
        comment_rows = c.fetchall()
        
        with open(comments_csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([description[0] for description in c.description])
            writer.writerows(comment_rows)
            
        conn.close()
        print(f"CSV export completed for {channel}")
    except Exception as e:
        print(f"Error exporting to CSV for channel {channel}: {e}")

async def export_to_json(channel):
    channel_dir = os.path.join(os.getcwd(), channel)
    db_file = os.path.join(channel_dir, f'{channel}.db')
    json_file = os.path.join(channel_dir, f'{channel}.json')
    
    if not os.path.exists(db_file):
        print(f"No database file found for channel {channel}")
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
            'channel': channel,
            'messages': messages,
            'comments': comments
        }
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        conn.close()
        print(f"JSON export completed for {channel}")
    except Exception as e:
        print(f"Error exporting to JSON for channel {channel}: {e}")

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
            driver = setup_neo4j_connection()
            if driver:
                try:
                    await upload_to_neo4j(driver)
                finally:
                    driver.close()
            
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

def setup_neo4j_connection():
    """Set up connection to Neo4j database"""
    if state.get('neo4j', {}).get('url') and state['neo4j'].get('password'):
        return try_neo4j_connection(
            state['neo4j']['url'],
            state['neo4j']['password'],
            state['neo4j'].get('database', 'neo4j')
        )
    
    db_type = input("Connect to Local [L] or Remote [R] Neo4j database? ").lower()
    while db_type not in ['l', 'r']:
        db_type = input("Please enter L for Local or R for Remote: ").lower()
    
    database = input("Enter database name (press Enter for 'neo4j'): ").strip()
    if not database:
        database = 'neo4j'
    
    if db_type == 'l':
        url = "bolt://localhost:7687"
        password = input("Enter database password: ")
    else:
        url = input("Enter full database URL (e.g., bolt://example.com:7687): ")
        password = input("Enter database password: ")
    
    # Save to state
    state['neo4j'] = {
        'type': 'local' if db_type == 'l' else 'remote',
        'url': url,
        'database': database,
        'password': password
    }
    save_state(state)
    
    return try_neo4j_connection(url, password, database)

def try_neo4j_connection(url, password, database):
    """Test Neo4j connection and return driver if successful"""
    try:
        driver = GraphDatabase.driver(url, auth=("neo4j", password))
        # Verify connection
        with driver.session(database=database) as session:
            session.run("RETURN 1")
        print("Successfully connected to Neo4j database!")
        return driver
    except Exception as e:
        print(f"Failed to connect to Neo4j: {str(e)}")
        # Clear saved connection details on failure
        if 'neo4j' in state:
            del state['neo4j']
            save_state(state)
        return None

async def upload_to_neo4j(driver):
    """Upload data to Neo4j using the provided schema"""
    if not driver:
        print("No valid Neo4j connection.")
        return

    try:
        # Create constraints and indexes for better performance
        with driver.session() as session:
            # Create constraints
            constraints = [
                "CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.userId IS UNIQUE",
                "CREATE CONSTRAINT channel_id IF NOT EXISTS FOR (c:Channel) REQUIRE c.channelId IS UNIQUE",
                "CREATE CONSTRAINT message_id IF NOT EXISTS FOR (m:Message) REQUIRE (m.channelId, m.messageId) IS UNIQUE",
                "CREATE CONSTRAINT comment_id IF NOT EXISTS FOR (c:Comment) REQUIRE c.commentId IS UNIQUE"
            ]
            
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    print(f"Constraint creation warning (may already exist): {e}")

        # Process each channel
        for channel_id in state['channels']:
            channel_dir = os.path.join(os.getcwd(), str(channel_id))
            db_file = os.path.join(channel_dir, f'{channel_id}.db')
            
            if not os.path.exists(db_file):
                print(f"No database file found for channel {channel_id}")
                continue

            print(f"\nProcessing channel {channel_id}...")
            
            # Connect to SQLite database
            conn = sqlite3.connect(db_file)
            c = conn.cursor()
            
            # Get channel details
            channel_details = state.get('channel_details', {}).get(channel_id, {})
            channel_name = channel_details.get('title', str(channel_id))
            channel_username = channel_details.get('username')
            
            with driver.session() as session:
                # Create Channel node
                session.run("""
                    MERGE (c:Channel {channelId: $channelId})
                    SET c.channelName = $channelName,
                        c.username = $username
                """, channelId=channel_id, channelName=channel_name, username=channel_username)
                
                # Process messages
                c.execute('''SELECT * FROM messages''')
                messages = c.fetchall()
                columns = [description[0] for description in c.description]
                
                # Create a set to track unique users
                unique_users = set()
                
                # Process each message
                for message in messages:
                    msg_dict = dict(zip(columns, message))
                    
                    # Create User node if not exists
                    if msg_dict['sender_id']:
                        unique_users.add((
                            msg_dict['sender_id'],
                            msg_dict['first_name'],
                            msg_dict['last_name'],
                            msg_dict['username']
                        ))
                    
                    # Create Message node and relationships
                    session.run("""
                        MERGE (m:Message {channelId: $channelId, messageId: $messageId})
                        SET m.timestamp = $timestamp,
                            m.text_content = $text_content,
                            m.media_type = $media_type,
                            m.media_path = $media_path,
                            m.mime_type = $mime_type
                        WITH m
                        MATCH (c:Channel {channelId: $channelId})
                        MERGE (m)-[:BELONGS_TO]->(c)
                        WITH m
                        MATCH (u:User {userId: $userId})
                        MERGE (u)-[:POSTED]->(m)
                    """, channelId=channel_id, messageId=str(msg_dict['message_id']),
                         timestamp=msg_dict['date'],
                         text_content=msg_dict['message'],
                         media_type=msg_dict['media_type'],
                         media_path=msg_dict['media_path'],
                         mime_type=msg_dict['mime_type'],
                         userId=str(msg_dict['sender_id']) if msg_dict['sender_id'] else None)
                
                # Create all unique User nodes
                for user in unique_users:
                    session.run("""
                        MERGE (u:User {userId: $userId})
                        SET u.first_name = $firstName,
                            u.last_name = $lastName,
                            u.username = $username
                        WITH u
                        MATCH (c:Channel {channelId: $channelId})
                        MERGE (u)-[:BELONGS_TO]->(c)
                    """, userId=str(user[0]),
                         firstName=user[1],
                         lastName=user[2],
                         username=user[3],
                         channelId=channel_id)
                
                # Process comments
                c.execute('''SELECT * FROM comments''')
                comments = c.fetchall()
                comment_columns = [description[0] for description in c.description]
                
                # Process each comment
                for comment in comments:
                    comment_dict = dict(zip(comment_columns, comment))
                    
                    # Create Comment node and relationships
                    session.run("""
                        MERGE (c:Comment {commentId: $commentId})
                        SET c.timestamp = $timestamp,
                            c.text_content = $text_content
                        WITH c
                        MATCH (m:Message {channelId: $channelId, messageId: $messageId})
                        MERGE (c)-[:REPLIES_TO]->(m)
                        WITH c
                        MATCH (u:User {userId: $userId})
                        MERGE (u)-[:COMMENTED]->(c)
                        WITH c
                        MATCH (ch:Channel {channelId: $channelId})
                        MERGE (c)-[:BELONGS_TO]->(ch)
                    """, commentId=str(comment_dict['comment_id']),
                         timestamp=comment_dict['date'],
                         text_content=comment_dict['comment_text'],
                         channelId=channel_id, messageId=str(comment_dict['message_id']),
                         userId=str(comment_dict['sender_id']) if comment_dict['sender_id'] else None)
            
            conn.close()
            print(f"Completed processing channel {channel_id}")
        
        print("\nNeo4j upload completed successfully!")
        
    except Exception as e:
        print(f"Error during Neo4j upload: {e}")
        raise

def get_media_files(channel_id):
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
        full_path = os.path.join(media_dir, media_path)
        if os.path.exists(full_path):
            files_to_process.append(media_path)
            print(f"Found file to transcribe: {media_path} (Type: {media_type}, MIME: {mime_type})")
        else:
            print(f"File not found: {media_path}")
    
    conn.close()
    
    return files_to_process

async def transcribe_media(channel_id):
    """Transcribe audio and video files for a channel"""
    try:
        print(f"\nInitializing Whisper model ({state['whisper_model']})...")
        model = whisper.load_model(state['whisper_model'])
        
        files = get_media_files(channel_id)
        if not files:
            print("No new media files to transcribe.")
            return
        
        print(f"Found {len(files)} files to transcribe.")
        channel_dir = os.path.join(os.getcwd(), str(channel_id))
        media_dir = os.path.join(channel_dir, 'media')
        
        # Connect to database
        db_file = os.path.join(channel_dir, f'{channel_id}.db')
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        
        for i, media_path in enumerate(files, 1):
            full_path = os.path.join(media_dir, media_path)
            print(f"\nProcessing file {i}/{len(files)}: {media_path}")
            
            try:
                # Transcribe the file
                result = model.transcribe(full_path)
                transcript = result["text"].strip()
                
                # Update database with transcript
                c.execute('''
                    UPDATE messages 
                    SET transcript = ? 
                    WHERE media_path = ?
                ''', (transcript, media_path))
                conn.commit()
                
                print(f"Transcription successful: {len(transcript)} characters")
                
            except Exception as e:
                print(f"Error transcribing {media_path}: {e}")
                continue
        
        conn.close()
        print("\nTranscription complete!")
        
    except Exception as e:
        print(f"Error during transcription: {e}")

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
    await main_menu()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted. Exiting...")
        sys.exit()
