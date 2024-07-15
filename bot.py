import os
import asyncio
import logging
from pyrogram import Client, filters
from pyrogram.types import Message
from aiohttp import ClientSession
import ffmpeg

# Configuration
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
DATABASE_DUMP_CHANNEL = os.getenv('DATABASE_DUMP_CHANNEL')
SUDO_USERS = list(map(int, os.getenv('SUDO_USERS', '').split(',')))
FFMPEG_BIN = os.getenv('FFMPEG_BIN', 'ffmpeg')
SPLIT_SIZE_MB = 1800  # 1.8 GB in MB

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Client("live_recording_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Helper functions
async def download_and_mux(url: str, chat_id: int, message_id: int, caption: str):
    """Download the M3U8 stream and mux it to MKV format with real-time processing."""
    # Construct the output file name
    output_file = f"{chat_id}_{message_id}.mkv"

    # Run the FFmpeg command
    command = [
        FFMPEG_BIN,
        '-i', url,
        '-c', 'copy',
        '-map', '0',
        '-f', 'matroska',
        '-y', output_file
    ]

    process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

    # Process stdout and stderr
    while True:
        stdout_line = await process.stdout.readline()
        stderr_line = await process.stderr.readline()

        if not stdout_line and not stderr_line and process.returncode is not None:
            break

        if stdout_line:
            logger.info(stdout_line.decode().strip())

        if stderr_line:
            logger.error(stderr_line.decode().strip())

    # Send the file to the chat
    with open(output_file, 'rb') as f:
        await app.send_document(chat_id, f, caption=caption)

    # Check file size and split if necessary
    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)  # Convert to MB
    if file_size_mb > SPLIT_SIZE_MB:
        # Implement file splitting logic
        await split_file(output_file)

    # Clean up
    os.remove(output_file)

async def split_file(file_path: str):
    """Split the MKV file if it exceeds the defined size limit."""
    output_files = []
    file_size = os.path.getsize(file_path)
    num_parts = (file_size // (SPLIT_SIZE_MB * 1024 * 1024)) + 1  # Calculate the number of parts

    for i in range(num_parts):
        part_file = f"{file_path}_part{i + 1}.mkv"
        output_files.append(part_file)
        command = [
            FFMPEG_BIN,
            '-i', file_path,
            '-c', 'copy',
            '-map', '0',
            '-f', 'segment',
            '-segment_time', '3600',
            '-segment_format', 'matroska',
            '-y', part_file
        ]
        await asyncio.create_subprocess_exec(*command)

    return output_files

@app.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    """Handle the /start command."""
    await message.reply_text("Welcome to the Live Recording Bot! Send an M3U8 link to start recording.")

@app.on_message(filters.text & filters.group)
async def text_handler(client: Client, message: Message):
    """Handle incoming messages in the group."""
    if message.from_user.id in SUDO_USERS:
        if message.text.startswith("http://") or message.text.startswith("https://"):
            await download_and_mux(message.text, message.chat.id, message.message_id, "Recording live stream")
        else:
            await message.reply_text("Please send a valid M3U8 link.")

@app.on_message(filters.command("dumpdb") & filters.user(SUDO_USERS))
async def dumpdb_handler(client: Client, message: Message):
    """Handle the /dumpdb command."""
    # Logic to dump the database goes here
    await message.reply_text(f"Dumping database to channel {DATABASE_DUMP_CHANNEL}.")

if __name__ == "__main__":
    app.run()
