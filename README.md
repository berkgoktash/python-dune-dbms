# Dune Archive System

A simple database system that supports creating types (tables), adding records, searching records by primary key, and deleting records.

## Requirements

- Python 3.x
- No additional packages required (uses only standard library)

## File Structure
- `archive.py`: Main program file
- `catalog.dat`: System catalog file (created automatically)
- `log.csv`: Operation log file (created automatically)
- `output.txt`: Search results output file (created automatically)

## Running the Program

1. Create an input file with commands (e.g., `input.txt`). Each line should contain one of the following commands:

```
create type <type-name> <number-of-fields> <primary-key-order> <field1-name> <field1-type> ...
create record <type-name> <field1-value> <field2-value> ...
search record <type-name> <primary-key>
delete record <type-name> <primary-key>
```

2. Run the program:
```bash
python3 archive.py input.txt
```

## Output

- Search results are written to `output.txt`
- All operations are logged to `log.csv` with timestamp, operation, and status
- Failed operations are logged as "failure"
- Successful operations are logged as "success"