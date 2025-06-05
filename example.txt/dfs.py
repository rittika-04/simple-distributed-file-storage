import os
import json

class DistributedFileStorageSystem:
    def __init__(self, nodes_dirs, metadata_file='metadata.json', chunk_size=1024 * 1024):
        """
        nodes_dirs: List of directories representing storage nodes
        metadata_file: File to store metadata about chunks and files
        chunk_size: size of each chunk in bytes (default 1MB)
        """
        self.nodes = nodes_dirs
        self.metadata_file = metadata_file
        self.chunk_size = chunk_size
        self.metadata = {}
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                self.metadata = json.load(f)

        # Create node directories if not exist
        for node in self.nodes:
            os.makedirs(node, exist_ok=True)

    def save_metadata(self):
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, f, indent=4)

    def store_file(self, filepath):
        filename = os.path.basename(filepath)
        file_size = os.path.getsize(filepath)
        chunks = []
        with open(filepath, 'rb') as f:
            chunk_num = 0
            while True:
                chunk_data = f.read(self.chunk_size)
                if not chunk_data:
                    break
                node = self.nodes[chunk_num % len(self.nodes)]
                chunk_filename = f"{filename}.chunk{chunk_num}"
                chunk_path = os.path.join(node, chunk_filename)
                with open(chunk_path, 'wb') as chunk_file:
                    chunk_file.write(chunk_data)
                chunks.append({
                    'node': node,
                    'chunk_filename': chunk_filename
                })
                chunk_num += 1

        # Save metadata
        self.metadata[filename] = {
            'size': file_size,
            'chunks': chunks
        }
        self.save_metadata()
        print(f"Stored file '{filename}' in {len(chunks)} chunks.")

    def retrieve_file(self, filename, output_path):
        if filename not in self.metadata:
            print(f"File '{filename}' not found in metadata.")
            return False

        chunks = self.metadata[filename]['chunks']
        with open(output_path, 'wb') as out_file:
            for chunk_info in chunks:
                chunk_path = os.path.join(chunk_info['node'], chunk_info['chunk_filename'])
                if not os.path.exists(chunk_path):
                    print(f"Missing chunk: {chunk_path}")
                    return False
                with open(chunk_path, 'rb') as chunk_file:
                    out_file.write(chunk_file.read())
        print(f"Retrieved file '{filename}' to '{output_path}'")
        return True

    def list_files(self):
        return list(self.metadata.keys())

# Example usage:
if __name__ == "__main__":
    # Define 3 storage nodes as folders
    nodes = ['node1_storage', 'node2_storage', 'node3_storage']

    dfs = DistributedFileStorageSystem(nodes)

    # Store a file
    file_to_store = 'example.txt'  # Replace with your file path
    if os.path.exists(file_to_store):
        dfs.store_file(file_to_store)

        # List stored files
        print("Files stored:", dfs.list_files())

        # Retrieve the file
        dfs.retrieve_file('example.txt', 'retrieved_example.txt')
    else:
        print(f"File {file_to_store} not found. Please add a file named 'example.txt' in this directory.")
