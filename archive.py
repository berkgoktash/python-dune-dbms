import os
import sys
import time
import struct
import csv
import re
from typing import Dict, List, Tuple, Optional, Any

class DuneArchive:
    def __init__(self):
        # System constants
        self.PAGE_SIZE = 4096  # bytes
        self.MAX_RECORDS_PER_PAGE = 10 # up to 10 records per page
        self.MAX_PAGES_PER_FILE = 100 # decided to include up to 100 pages per file for the sake of simplicity and performance
        self.MAX_FIELDS_PER_TYPE = 10 # at least 6 fields
        self.MAX_TYPE_NAME_LENGTH = 12 # at least 12 characters
        self.MAX_FIELD_NAME_LENGTH = 20 # at least 20 characters
        self.MAX_STRING_LENGTH = 100 # decided to include up to 100 characters
        
        # System catalog
        self.catalog = {}  # type_name -> type_info
        self.catalog_file = "catalog.dat"
        
        # Load existing catalog
        self._load_catalog()
    
    def _load_catalog(self):
        # Load the system catalog
        if os.path.exists(self.catalog_file):
            try:
                with open(self.catalog_file, 'rb') as f:
                    data = f.read()
                    if data:
                        # Simple serialization: each type is stored as:
                        # type_name_length(4) + type_name + num_fields(4) + primary_key_order(4) + 
                        # for each field: field_name_length(4) + field_name + field_type_length(4) + field_type
                        offset = 0
                        while offset < len(data):
                            # Read type name
                            type_name_len = struct.unpack('I', data[offset:offset+4])[0]
                            offset += 4
                            type_name = data[offset:offset+type_name_len].decode('utf-8')
                            offset += type_name_len
                            
                            # Read number of fields and primary key order
                            num_fields = struct.unpack('I', data[offset:offset+4])[0]
                            offset += 4
                            primary_key_order = struct.unpack('I', data[offset:offset+4])[0]
                            offset += 4
                            
                            # Read fields (name, type)
                            fields = []
                            for i in range(num_fields):
                                field_name_len = struct.unpack('I', data[offset:offset+4])[0]
                                offset += 4
                                field_name = data[offset:offset+field_name_len].decode('utf-8')
                                offset += field_name_len
                                
                                field_type_len = struct.unpack('I', data[offset:offset+4])[0]
                                offset += 4
                                field_type = data[offset:offset+field_type_len].decode('utf-8')
                                offset += field_type_len
                                
                                fields.append((field_name, field_type))
                            
                            # Add to catalog
                            self.catalog[type_name] = {
                                'fields': fields,
                                'primary_key_order': primary_key_order,
                                'record_size': self._calculate_record_size(fields)
                            }
            except:
                pass 
    
    def _save_catalog(self):
        # Save the system catalog
        with open(self.catalog_file, 'wb') as f:
            for type_name, type_info in self.catalog.items():
                # Write type name
                type_name_bytes = type_name.encode('utf-8')
                f.write(struct.pack('I', len(type_name_bytes)))
                f.write(type_name_bytes)
                
                # Write number of fields and primary key order
                f.write(struct.pack('I', len(type_info['fields'])))
                f.write(struct.pack('I', type_info['primary_key_order']))
                
                # Write fields
                for field_name, field_type in type_info['fields']:
                    field_name_bytes = field_name.encode('utf-8')
                    field_type_bytes = field_type.encode('utf-8')
                    
                    f.write(struct.pack('I', len(field_name_bytes)))
                    f.write(field_name_bytes)
                    f.write(struct.pack('I', len(field_type_bytes)))
                    f.write(field_type_bytes)
    
    def _calculate_record_size(self, fields: List[Tuple[str, str]]) -> int:
        # Calculate the fixed size of a record based on its fields
        size = 1  # validity flag (1 byte)
        for field_name, field_type in fields:
            if field_type == 'int':
                size += 4  # 4 bytes for integer
            elif field_type == 'str':
                size += self.MAX_STRING_LENGTH  # fixed size for strings
        return size
    
    def _get_data_file_path(self, type_name: str) -> str:
        # Get the data file path for a given type
        return f"{type_name}.dat"
    
    def _create_page_header(self, page_number: int, num_records: int, bitmap: int) -> bytes:
        # Create a page header (12 bytes)
        # page_number(4) + num_records(4) + bitmap(4)
        return struct.pack('III', page_number, num_records, bitmap)
    
    def _parse_page_header(self, header_data: bytes) -> Tuple[int, int, int]:
        # Parse page header
        return struct.unpack('III', header_data[:12])
    
    def _serialize_record(self, type_name: str, values: List[str]) -> bytes:
        # Serialize a record to bytes
        type_info = self.catalog[type_name]
        fields = type_info['fields']
        
        # Start with validity flag (1 = valid)
        record_data = struct.pack('B', 1)
        
        for i, (field_name, field_type) in enumerate(fields):
            if field_type == 'int':
                value = int(values[i])
                record_data += struct.pack('i', value)
            elif field_type == 'str':
                value = values[i][:self.MAX_STRING_LENGTH-1]  # Ensure it fits
                # Pad string to fixed length
                padded_value = value.ljust(self.MAX_STRING_LENGTH, '\0')
                record_data += padded_value.encode('utf-8')
        
        return record_data
    
    def _deserialize_record(self, type_name: str, record_data: bytes) -> List[str]:
        # Deserialize a record from bytes
        type_info = self.catalog[type_name]
        fields = type_info['fields']
        
        # Check validity flag
        validity = struct.unpack('B', record_data[:1])[0]
        if validity == 0:
            return None  # Invalid record
        
        values = []
        offset = 1  # Skip validity flag
        
        for field_name, field_type in fields:
            if field_type == 'int':
                value = struct.unpack('i', record_data[offset:offset+4])[0]
                values.append(str(value))
                offset += 4
            elif field_type == 'str':
                value_bytes = record_data[offset:offset+self.MAX_STRING_LENGTH]
                value = value_bytes.decode('utf-8').rstrip('\0')
                values.append(value)
                offset += self.MAX_STRING_LENGTH
        
        return values
    
    def _load_page(self, type_name: str, page_number: int) -> Tuple[Optional[bytes], int, int]:
        # Load a specific page from file
        file_path = self._get_data_file_path(type_name)
        if not os.path.exists(file_path):
            return None, 0, 0
        
        with open(file_path, 'rb') as f:
            # Seek to the page
            page_offset = page_number * self.PAGE_SIZE
            f.seek(page_offset)
            
            page_data = f.read(self.PAGE_SIZE)
            if len(page_data) < 12:  # Not enough data for header
                return None, 0, 0
            
            # Parse header
            page_num, num_records, bitmap = self._parse_page_header(page_data)
            return page_data, num_records, bitmap
    
    def _save_page(self, type_name: str, page_number: int, page_data: bytes):
        # Save a page to file
        file_path = self._get_data_file_path(type_name)
        
        # Create file if it doesn't exist or extend it if necessary
        if not os.path.exists(file_path):
            with open(file_path, 'wb') as f:
                f.write(b'\0' * ((page_number + 1) * self.PAGE_SIZE))
        else:
            # Check if we need to extend the file
            current_size = os.path.getsize(file_path)
            required_size = (page_number + 1) * self.PAGE_SIZE
            if current_size < required_size:
                with open(file_path, 'ab') as f:
                    f.write(b'\0' * (required_size - current_size))
        
        # Write the page
        with open(file_path, 'r+b') as f:
            page_offset = page_number * self.PAGE_SIZE
            f.seek(page_offset)
            f.write(page_data)
    
    def _find_primary_key_value(self, type_name: str, values: List[str]) -> str:
        # Extract the primary key value from record values
        type_info = self.catalog[type_name]
        primary_key_order = type_info['primary_key_order']
        return values[primary_key_order - 1]  # Convert to 0-based index
    
    def _log_operation(self, operation: str, success: bool):
        # Log an operation to the CSV log file
        timestamp = int(time.time()) 
        status = "success" if success else "failure"
        
        # Append to log file
        with open('log.csv', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([f"{timestamp}", f" {operation}", f" {status}"])
    
    def _is_valid_name(self, name: str) -> bool:
        # Validate type names and field names
        # Must contain at least one letter and only letters/digits
        return bool(re.match(r'^(?=.*[a-zA-Z])[a-zA-Z0-9]+$', name))

    def _is_valid_string_value(self, value: str) -> bool:
        # Validate string field values
        # Only letters and digits allowed
        return bool(re.match(r'^[a-zA-Z0-9]+$', value))

    def _is_valid_int_value(self, value: str) -> bool:
        # Validate integer field values
        try:
            int(value)
            return True
        except ValueError:
            return False

    def create_type(self, type_name: str, num_fields: int, primary_key_order: int, field_specs: List[str]) -> bool:
        # Create a new type (table)
        # Check if type already exists
        if type_name in self.catalog:
            return False
        
        # Validate type name
        if not self._is_valid_name(type_name):
            return False
        
        # Validate inputs
        if len(type_name) > self.MAX_TYPE_NAME_LENGTH:
            return False
        if num_fields > self.MAX_FIELDS_PER_TYPE:
            return False
        if primary_key_order < 1 or primary_key_order > num_fields:
            return False
        
        # Parse field specifications
        fields = []
        for i in range(0, len(field_specs), 2):
            field_name = field_specs[i]
            field_type = field_specs[i + 1]
            
            # Validate field name
            if not self._is_valid_name(field_name):
                return False
            
            if len(field_name) > self.MAX_FIELD_NAME_LENGTH:
                return False
            if field_type not in ['int', 'str']:
                return False
            
            fields.append((field_name, field_type))
        
        if len(fields) != num_fields:
            return False
        
        # Add to catalog
        self.catalog[type_name] = {
            'fields': fields,
            'primary_key_order': primary_key_order,
            'record_size': self._calculate_record_size(fields)
        }
        
        # Save catalog
        self._save_catalog()
        
        return True
    
    def create_record(self, type_name: str, values: List[str]) -> bool:
        # Create a new record
        if type_name not in self.catalog:
            return False
        
        type_info = self.catalog[type_name]
        if len(values) != len(type_info['fields']):
            return False
        
        # Validate values match their field types
        for i, ((field_name, field_type), value) in enumerate(zip(type_info['fields'], values)):
            if field_type == 'int':
                if not self._is_valid_int_value(value):
                    return False
            elif field_type == 'str':
                if not self._is_valid_string_value(value):
                    return False
        
        # Get primary key value
        primary_key_value = self._find_primary_key_value(type_name, values)
        
        # Check if record with this primary key already exists
        if self._search_record_internal(type_name, primary_key_value) is not None:
            return False
        
        # Serialize the record
        record_data = self._serialize_record(type_name, values)
        
        # Find a page with free space or create a new one
        page_number = 0
        while True:
            page_data, num_records, bitmap = self._load_page(type_name, page_number)
            
            if page_data is None:
                # Create new page
                bitmap = 0
                num_records = 0
                page_data = bytearray(self.PAGE_SIZE)
                # Initialize with header
                header = self._create_page_header(page_number, 0, 0)
                page_data[:12] = header
            else:
                page_data = bytearray(page_data)
            
            # Find free slot
            free_slot = None
            for slot in range(self.MAX_RECORDS_PER_PAGE):
                if not (bitmap & (1 << slot)): # If the slot is not occupied (bitmap is a bitmask)
                    free_slot = slot
                    break
            
            if free_slot is not None:
                # Add record to this slot
                record_offset = 12 + free_slot * type_info['record_size']
                page_data[record_offset:record_offset + len(record_data)] = record_data
                
                # Update bitmap and header
                bitmap = bitmap | (1 << free_slot) # Set the bit at the free slot to 1
                num_records += 1
                header = self._create_page_header(page_number, num_records, bitmap)
                page_data[:12] = header
                
                # Save page
                self._save_page(type_name, page_number, bytes(page_data))
                return True
            
            page_number += 1
            if page_number >= self.MAX_PAGES_PER_FILE:
                return False
    
    def _search_record_internal(self, type_name: str, primary_key: str) -> Optional[List[str]]:
        # Internal method to search for a record
        if type_name not in self.catalog:
            return None
        
        type_info = self.catalog[type_name]
        primary_key_index = type_info['primary_key_order'] - 1
        
        # Search through all pages
        page_number = 0
        while page_number < self.MAX_PAGES_PER_FILE:
            page_data, num_records, bitmap = self._load_page(type_name, page_number)
            
            if page_data is None or num_records == 0:
                page_number += 1
                continue
            
            # Check each slot
            for slot in range(self.MAX_RECORDS_PER_PAGE):
                if bitmap & (1 << slot):  # If the slot is not occupied (bitmap is a bitmask)
                    record_offset = 12 + slot * type_info['record_size'] # Calculate the offset of the record (12 bytes for the header)
                    record_data = page_data[record_offset:record_offset + type_info['record_size']]
                    
                    values = self._deserialize_record(type_name, record_data)
                    if values and values[primary_key_index] == primary_key:
                        return values
            
            page_number += 1
        
        return None
    
    def search_record(self, type_name: str, primary_key: str) -> Optional[List[str]]:
        # Search for a record by primary key
        return self._search_record_internal(type_name, primary_key)
    
    def delete_record(self, type_name: str, primary_key: str) -> bool:
        # Delete a record by primary key
        if type_name not in self.catalog:
            return False
        
        type_info = self.catalog[type_name]
        primary_key_index = type_info['primary_key_order'] - 1
        
        # Search through all pages
        page_number = 0
        while page_number < self.MAX_PAGES_PER_FILE:
            page_data, num_records, bitmap = self._load_page(type_name, page_number)
            
            if page_data is None or num_records == 0:
                page_number += 1
                continue
            
            page_data = bytearray(page_data)
            
            # Check each slot
            for slot in range(self.MAX_RECORDS_PER_PAGE):
                if bitmap & (1 << slot):  # If the slot is not occupied (bitmap is a bitmask)
                    record_offset = 12 + slot * type_info['record_size'] # Calculate the offset of the record (12 bytes for the header)
                    record_data = page_data[record_offset:record_offset + type_info['record_size']]
                    
                    values = self._deserialize_record(type_name, record_data)
                    if values and values[primary_key_index] == primary_key:
                        # Mark record as invalid
                        page_data[record_offset] = 0  # Set validity flag to 0
                        
                        # Update bitmap and header
                        bitmap = bitmap & ~(1 << slot)  # Set the bit at the slot to 0
                        num_records -= 1
                        header = self._create_page_header(page_number, num_records, bitmap)
                        page_data[:12] = header
                        
                        # Save page
                        self._save_page(type_name, page_number, bytes(page_data))
                        return True
            
            page_number += 1
        
        return False

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 archive.py <input_file_path>")
        return
    
    input_file_path = sys.argv[1]
    archive = DuneArchive()
    
    # Clear output file
    with open('output.txt', 'w') as f:
        pass
    
    try:
        with open(input_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split()
                if len(parts) < 2:  # Invalid command format
                    archive._log_operation(line, False)
                    continue
                
                operation_type = parts[0]
                operation = parts[1]
                
                # Handle create operations
                if operation_type == 'create':
                    if operation == 'type':
                        if len(parts) < 5:  # Not enough parameters for type creation
                            archive._log_operation(line, False)
                            continue
                        try:
                            type_name = parts[2]
                            num_fields = int(parts[3])
                            primary_key_order = int(parts[4])
                            field_specs = parts[5:]
                            
                            if len(field_specs) != 2 * num_fields:  # Invalid number of field specifications
                                archive._log_operation(line, False)
                                continue
                            
                            success = archive.create_type(type_name, num_fields, primary_key_order, field_specs)
                            archive._log_operation(line, success)
                        except (ValueError, IndexError):
                            archive._log_operation(line, False)
                            continue
                            
                    elif operation == 'record':
                        if len(parts) < 3:  # Not enough parameters for record creation
                            archive._log_operation(line, False)
                            continue
                        type_name = parts[2]
                        values = parts[3:]
                        
                        if type_name not in archive.catalog:  # Type doesn't exist
                            archive._log_operation(line, False)
                            continue
                            
                        if len(values) != len(archive.catalog[type_name]['fields']):  # Wrong number of values
                            archive._log_operation(line, False)
                            continue
                            
                        success = archive.create_record(type_name, values)
                        archive._log_operation(line, success)
                    else:
                        archive._log_operation(line, False)  # Invalid create operation
                
                # Handle search operation
                elif operation_type == 'search' and operation == 'record':
                    if len(parts) != 4:  # Wrong number of parameters for search
                        archive._log_operation(line, False)
                        continue
                        
                    type_name = parts[2]
                    primary_key = parts[3]
                    
                    if type_name not in archive.catalog:  # Type doesn't exist
                        archive._log_operation(line, False)
                        continue
                    
                    result = archive.search_record(type_name, primary_key)
                    success = result is not None
                    
                    if success:
                        # Write result to output file
                        with open('output.txt', 'a') as out_f:
                            out_f.write(' '.join(result) + '\n')
                    
                    archive._log_operation(line, success)
                
                # Handle delete operation
                elif operation_type == 'delete' and operation == 'record':
                    if len(parts) != 4:  # Wrong number of parameters for delete
                        archive._log_operation(line, False)
                        continue
                        
                    type_name = parts[2]
                    primary_key = parts[3]
                    
                    if type_name not in archive.catalog:  # Type doesn't exist
                        archive._log_operation(line, False)
                        continue
                    
                    success = archive.delete_record(type_name, primary_key)
                    archive._log_operation(line, success)
                
                # Handle invalid operation type
                else:
                    archive._log_operation(line, False)
    
    except Exception as e:
        print(f"Error processing input file: {e}")

if __name__ == "__main__":
    main()
