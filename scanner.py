#!/usr/bin/env python3
"""
Obsidian File Watcher with ChromaDB Vector Database Indexing
Watches for changes in Obsidian vault and indexes content in overlapping chunks
"""

import os
import sys
import time
import hashlib
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json

import chromadb
from chromadb.config import Settings
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileCreatedEvent, FileDeletedEvent

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
CHUNK_SIZE = 1024
OVERLAP_SIZE = 256
STEP_SIZE = CHUNK_SIZE - OVERLAP_SIZE
DB_NAME = "obsidian_notes"
COLLECTION_NAME = "note_chunks"
METADATA_FILE = ".indexer_metadata.json"


def find_obsidian_vault() -> Optional[Path]:
    """Discover Obsidian vault location on macOS"""
    possible_locations = [
        Path.home() / "Documents" / "Obsidian",
        Path.home() / "Library" / "Mobile Documents" / "iCloud~md~obsidian" / "Documents",
    ]
    
    # Check common locations
    for location in possible_locations:
        if location.exists() and location.is_dir():
            # Look for .obsidian folder
            for item in location.rglob(".obsidian"):
                if item.is_dir():
                    return item.parent
    
    # Search in Documents folder for any folder with .obsidian
    docs = Path.home() / "Documents"
    if docs.exists():
        for item in docs.iterdir():
            if item.is_dir() and (item / ".obsidian").exists():
                return item
    
    return None


def get_obsidian_path() -> Path:
    """Get Obsidian vault path from environment or discovery"""
    env_path = os.environ.get("OBSIDIAN_VAULT_PATH")
    if env_path:
        path = Path(env_path)
        if path.exists() and path.is_dir():
            logger.info(f"Using Obsidian vault from environment: {path}")
            return path
        else:
            logger.warning(f"Environment path {env_path} doesn't exist, falling back to discovery")
    
    discovered_path = find_obsidian_vault()
    if discovered_path:
        logger.info(f"Discovered Obsidian vault at: {discovered_path}")
        return discovered_path
    
    raise ValueError("Could not find Obsidian vault. Please set OBSIDIAN_VAULT_PATH environment variable.")


def calculate_file_hash(filepath: Path) -> str:
    """Calculate MD5 hash of file content"""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def chunk_content(title: str, content: str) -> List[Tuple[str, int, int]]:
    """
    Break content into overlapping chunks.
    Returns list of (chunk_text, start_pos, end_pos)
    """
    chunks = []
    
    # First chunk is just the title
    chunks.append((title, 0, 0))
    
    # Create overlapping chunks
    content_bytes = content.encode('utf-8')
    pos = 0
    
    while pos < len(content_bytes):
        end_pos = min(pos + CHUNK_SIZE, len(content_bytes))
        
        # Try to decode the chunk, handling potential UTF-8 boundary issues
        chunk_bytes = content_bytes[pos:end_pos]
        
        # Ensure we don't cut in the middle of a UTF-8 character
        while end_pos > pos:
            try:
                chunk_text = chunk_bytes.decode('utf-8')
                break
            except UnicodeDecodeError:
                end_pos -= 1
                chunk_bytes = content_bytes[pos:end_pos]
        
        if end_pos > pos:
            chunks.append((chunk_text, pos, end_pos))
        
        pos += STEP_SIZE
    
    return chunks


class ObsidianIndexer:
    def __init__(self, vault_path: Path, db_path: Path):
        self.vault_path = vault_path
        self.db_path = db_path
        self.metadata_file = db_path / METADATA_FILE
        
        # Initialize ChromaDB
        self.client = chromadb.PersistentClient(
            path=str(db_path),
            settings=Settings(anonymized_telemetry=False)
        )
        
        # Get or create collection
        try:
            self.collection = self.client.get_collection(COLLECTION_NAME)
            logger.info(f"Using existing collection: {COLLECTION_NAME}")
        except:
            self.collection = self.client.create_collection(
                name=COLLECTION_NAME,
                metadata={"hnsw:space": "cosine"}
            )
            logger.info(f"Created new collection: {COLLECTION_NAME}")
        
        # Load metadata
        self.file_hashes = self.load_metadata()
    
    def load_metadata(self) -> Dict[str, str]:
        """Load file hashes from metadata file"""
        if self.metadata_file.exists():
            with open(self.metadata_file, 'r') as f:
                return json.load(f)
        return {}
    
    def save_metadata(self):
        """Save file hashes to metadata file"""
        with open(self.metadata_file, 'w') as f:
            json.dump(self.file_hashes, f, indent=2)
    
    def remove_file_chunks(self, filepath: str):
        """Remove all chunks for a given file from the database"""
        try:
            # Get all chunks for this file
            results = self.collection.get(
                where={"file_path": filepath}
            )
            
            if results['ids']:
                self.collection.delete(ids=results['ids'])
                logger.info(f"Removed {len(results['ids'])} chunks for {filepath}")
        except Exception as e:
            logger.error(f"Error removing chunks for {filepath}: {e}")
    
    def index_file(self, filepath: Path):
        """Index a single markdown file"""
        if not filepath.suffix in ['.md', '.markdown']:
            return
        
        try:
            # Read file content
            content = filepath.read_text(encoding='utf-8')
            title = filepath.stem
            
            # Calculate hash
            current_hash = calculate_file_hash(filepath)
            relative_path = str(filepath.relative_to(self.vault_path))
            
            # Check if file has changed
            if relative_path in self.file_hashes and self.file_hashes[relative_path] == current_hash:
                logger.debug(f"File unchanged: {relative_path}")
                return
            
            # Remove old chunks if file existed before
            if relative_path in self.file_hashes:
                self.remove_file_chunks(relative_path)
            
            # Create chunks
            chunks = chunk_content(title, content)
            
            # Prepare data for ChromaDB
            ids = []
            documents = []
            metadatas = []
            
            for i, (chunk_text, start_pos, end_pos) in enumerate(chunks):
                chunk_id = f"{relative_path}_{i}"
                ids.append(chunk_id)
                documents.append(chunk_text)
                metadatas.append({
                    "file_path": relative_path,
                    "title": title,
                    "chunk_index": i,
                    "start_pos": start_pos,
                    "end_pos": end_pos,
                    "is_title_chunk": i == 0,
                    "full_path": str(filepath)
                })
            
            # Add to ChromaDB
            self.collection.add(
                ids=ids,
                documents=documents,
                metadatas=metadatas
            )
            
            # Update hash
            self.file_hashes[relative_path] = current_hash
            self.save_metadata()
            
            logger.info(f"Indexed {len(chunks)} chunks for {relative_path}")
            
        except Exception as e:
            logger.error(f"Error indexing {filepath}: {e}")
    
    def remove_file(self, filepath: Path):
        """Remove a file from the index"""
        try:
            relative_path = str(filepath.relative_to(self.vault_path))
            self.remove_file_chunks(relative_path)
            
            if relative_path in self.file_hashes:
                del self.file_hashes[relative_path]
                self.save_metadata()
            
            logger.info(f"Removed {relative_path} from index")
        except Exception as e:
            logger.error(f"Error removing {filepath}: {e}")
    
    def initial_index(self):
        """Perform initial indexing of all markdown files"""
        logger.info("Starting initial index...")
        md_files = list(self.vault_path.rglob("*.md")) + list(self.vault_path.rglob("*.markdown"))
        
        for i, filepath in enumerate(md_files):
            self.index_file(filepath)
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{len(md_files)} files indexed")
        
        logger.info(f"Initial index complete. Indexed {len(md_files)} files.")


class ObsidianEventHandler(FileSystemEventHandler):
    def __init__(self, indexer: ObsidianIndexer):
        self.indexer = indexer
        
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(('.md', '.markdown')):
            logger.info(f"File created: {event.src_path}")
            self.indexer.index_file(Path(event.src_path))
    
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith(('.md', '.markdown')):
            logger.info(f"File modified: {event.src_path}")
            self.indexer.index_file(Path(event.src_path))
    
    def on_deleted(self, event):
        if not event.is_directory and event.src_path.endswith(('.md', '.markdown')):
            logger.info(f"File deleted: {event.src_path}")
            self.indexer.remove_file(Path(event.src_path))


def main():
    # Get vault path
    vault_path = get_obsidian_path()
    
    # Set up database in script directory
    script_dir = Path(__file__).parent
    db_path = script_dir / "obsidian_chromadb"
    db_path.mkdir(exist_ok=True)
    
    # Create indexer
    indexer = ObsidianIndexer(vault_path, db_path)
    
    # Perform initial indexing
    indexer.initial_index()
    
    # Set up file watcher
    event_handler = ObsidianEventHandler(indexer)
    observer = Observer()
    observer.schedule(event_handler, str(vault_path), recursive=True)
    
    # Start watching
    observer.start()
    logger.info("File watcher started. Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(5)  # Light sleep to reduce CPU usage
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Stopping file watcher...")
    
    observer.join()
    logger.info("File watcher stopped.")


if __name__ == "__main__":
    main()