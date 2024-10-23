import hashlib
from typing import Dict, List

class DataChunk:
    def __init__(self, data: bytes):
        self.data = data
        self.hash = hashlib.sha256(data).hexdigest()

class DistributedStorage:
    def __init__(self, blockchain):
        self.blockchain = blockchain
        self.data_chunks: Dict[str, DataChunk] = {}
        self.file_mappings: Dict[str, List[str]] = {}

    def store_file(self, file_name: str, data: bytes) -> str:
        chunk_size = 1024 * 1024  # 1 MB chunks
        chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
        
        chunk_hashes = []
        for chunk in chunks:
            data_chunk = DataChunk(chunk)
            self.data_chunks[data_chunk.hash] = data_chunk
            chunk_hashes.append(data_chunk.hash)
        
        file_hash = hashlib.sha256(("".join(chunk_hashes)).encode()).hexdigest()
        self.file_mappings[file_hash] = chunk_hashes
        
        self.blockchain.add_new_block(f"File stored: {file_hash}", 2)  # Assume shard 2 for storage
        return file_hash

    def retrieve_file(self, file_hash: str) -> bytes:
        chunk_hashes = self.file_mappings.get(file_hash)
        if not chunk_hashes:
            return None
        
        file_data = b""
        for chunk_hash in chunk_hashes:
            chunk = self.data_chunks.get(chunk_hash)
            if chunk:
                file_data += chunk.data
            else:
                return None  # File is incomplete
        
        return file_data

    def delete_file(self, file_hash: str) -> bool:
        chunk_hashes = self.file_mappings.get(file_hash)
        if not chunk_hashes:
            return False
        
        for chunk_hash in chunk_hashes:
            if chunk_hash in self.data_chunks:
                del self.data_chunks[chunk_hash]
        
        del self.file_mappings[file_hash]
        self.blockchain.add_new_block(f"File deleted: {file_hash}", 2)
        return True