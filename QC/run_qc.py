import os

# Configuration
batch_dir = 'batch_files'
script_name = 'process_batch.py'  # This is the name of your script file
total_records = 33000
batch_size = 3000

# Create directory for batch files if it doesn't exist
os.makedirs(batch_dir, exist_ok=True)

# Generate batch files
for i in range(0, total_records, batch_size):
    start_id = i
    end_id = min(i + batch_size - 1, total_records - 1)
    batch_file_name = f'batch_{start_id}_{end_id}.bat'
    batch_file_path = os.path.join(batch_dir, batch_file_name)

    with open(batch_file_path, 'w') as batch_file:
        batch_file.write(f'python {script_name} {start_id} {end_id}\n')

    print(f'Created batch file: {batch_file_path}')

print('Batch files created successfully.')
