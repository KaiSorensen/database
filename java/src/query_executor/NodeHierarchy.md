QUERY
- Object Queery
  - create
    - extend (get existing object's attributes for new object)
  - read (to get metadata)
  - update (rename)
  - delete
- Attribute Query
  - create
  - read
  - update (rename)
  - delete
- Data Query
  - create
    - selection
      - row-wise operation
      - column-wise operation
  - read
    - selection
      - row-wise operation
      - column-wise operation
  - update
    - selection
      - row-wise operation
      - column-wise operation
  - delete
    - selection
      - row-wise operation
      - column-wise operation



### INPUTs & OUTPUTs

**Data Query Create**
  - input
    - tabular data
      - must represent full row(s) or full column(s)
  - output
    - success / fail

**Data Query Read**
  - input
    - tabular data
      - any selected shape is valid
  - output
    - tabular data

**Data Query Update**
  - input
    - tabular data
      - must represent existing location(s)
  - output
    - success / fail

**Data Query Delete**
  - input
    - tabular data
      - must represent full row(s) or full column(s)
  - output
    - success / fail

**Selection**  
  - input
    - attributes & filter
  - output
    - tabular data
  
**Row-Wise Operation**  
  - input
    - tabular data
  - output
    - single column of item(s)

**Column-Wise Operation**
  - input
    - single column of item(s)
      - (noteably still represented as tabular data)
  - output
    - single value
      - (noteably still represented as tabular data)


### NODE COMPATIBILITY

Based on the current input/output definitions above:

**Data Query Create**
  - possible input nodes
    - Selection
      - if the selection output represents full row(s) or full column(s)
    - Row-Wise Operation
      - if the output column is intended to become a full column
  - possible output nodes
    - none
      - output is success / fail

**Data Query Read**
  - possible input nodes
    - Selection
    - Row-Wise Operation
    - Column-Wise Operation
  - possible output nodes
    - Data Query Create
      - if the read result is full row(s) or full column(s)
    - Data Query Read
    - Data Query Update
      - if the read result represents existing location(s)
    - Data Query Delete
      - if the read result is full row(s) or full column(s)
    - Row-Wise Operation
    - Column-Wise Operation
      - if the read result is a single column

**Data Query Update**
  - possible input nodes
    - Selection
      - if the selection output represents existing location(s)
    - Row-Wise Operation
      - if the output values map to existing location(s)
    - Column-Wise Operation
      - if the single value maps to an existing location
  - possible output nodes
    - none
      - output is success / fail

**Data Query Delete**
  - possible input nodes
    - Selection
      - if the selection output represents full row(s) or full column(s)
    - Row-Wise Operation
      - if the output column still identifies a full deletable column
  - possible output nodes
    - none
      - output is success / fail

**Selection**
  - possible input nodes
    - none
      - under the current definition, selection takes attributes & filter rather than another node's output
  - possible output nodes
    - Data Query Create
      - if the selection output is full row(s) or full column(s)
    - Data Query Read
    - Data Query Update
      - if the selection output represents existing location(s)
    - Data Query Delete
      - if the selection output is full row(s) or full column(s)
    - Row-Wise Operation
    - Column-Wise Operation
      - if the selection output is a single column

**Row-Wise Operation**
  - possible input nodes
    - Data Query Read
    - Selection
    - Row-Wise Operation
    - Column-Wise Operation
      - if a single value is allowed as degenerate tabular data
  - possible output nodes
    - Data Query Create
      - if the output column is a full creatable column
    - Data Query Read
    - Data Query Update
      - if the output maps to existing location(s)
    - Data Query Delete
      - if the output column identifies a full deletable column
    - Row-Wise Operation
    - Column-Wise Operation

**Column-Wise Operation**
  - possible input nodes
    - Data Query Read
      - if the read result is a single column
    - Selection
      - if the selection output is a single column
    - Row-Wise Operation
  - possible output nodes
    - Data Query Read
    - Data Query Update
      - if the single value maps to an existing location
    - Row-Wise Operation
      - if a single value is allowed as degenerate tabular data

Note:
  - with the current definitions, `Selection` cannot yet take another node's tabular output as input
  - so the filter / operate loop is only partial at the moment
  - if you want a full recursive loop between filtering and operating, `Selection` will likely need to accept tabular data as input in addition to attributes & filter
