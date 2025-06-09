Input File Explanation:

1) create type house 6 1 name str origin str leader str military_strength int wealth int spice_production int
   - Defines a new type called 'house' with 6 fields.
   - Primary key is the first field: 'name'.
   - Fields and their types are listed.

2) create record house Atreides Caladan Duke 8000 5000 150
   - Creates a record for type 'house' with field values.

3) create record house Harkonnen GiediPrime Baron 12000 3000 200
   - Creates another 'house' record.

4) create type fremen 5 1 name str tribe str skill_level int allegiance str age int
   - Defines a new type called 'fremen'.

5) create record fremen Stilgar SietchTabr 9 Atreides 45
   - Adds a record to 'fremen'.

6) create record fremen Chani SietchTabr 8 Atreides 30
   - Adds another 'fremen' record.

7) delete record house Corrino
   - Attempts to delete a non-existing 'house' record. Will fail.

8) search record fremen Stilgar
   - Searches for 'Stilgar' in 'fremen'. Should output the full record.

9) search record house Atreides
   - Searches for 'Atreides' in 'house'. Should output the full record.

10) delete record fremen Chani
    - Deletes the record 'Chani' from 'fremen'.

11) search record fremen Chani
    - Attempts to search for deleted record 'Chani'. Will fail (no output).
