What is a parse tree and how does it work?

The language parser takes the language and converts it into a parse tree.

What is a parse tree? Why is it a tree?

Well, we need to have a hierarchy of actions. Theoretically, we could have a hierarchy of all possibilities. In SQL, this tree would be extremely complicated. In our case, it's manageable. For example, are we manipulating data? We we manipulating an attribute? Are we manupulating an object? Those can't overlap or occur in the same language statement. Therefore, they could be the tree roots. If editing data, are we updating/deleting? Are we filtering? How much data are we editing? This requires explicit examination. In a way it's like a grammar but for possible actions instead of possible syntax.

High Level: what is there to do?  
- there's CRUD at each level  
is there anything else?  


High Level could either be:  
```CREATE -- READ -- UPDATE -- DELETE```  
or  
```OBJECT -- ATTRIBUTE -- DATA```

Data is very nuanced because that's where filtering and batch operations occur. So, while it's still CRUD in essence, it's not simple.  

I feel like, due to data's complicated nature, we should approach it from ```OBJECT -- ATTRIBUTE -- DATA``` at the highest level, with CRUD underneath each, since CRUD works differently for each.  

So, ```OBJECT``` and ```ATTRIBUTE``` are easy.  

For ```DATA```, what are the high level options. Is it CRUD? Let's list a few and categorize them (with parameters).

- create a datapoint (object, attribute)
- update a datapoint (object, attribute)
- update all datapoints (object, attribute, filter)
- create a new attribute based on existing attributes (object, optional filter, operation (like an average))
- an operation can be needed for any of the CRUDs. Are they all filters? Not necessarily. The filters come first, the operation comes second, then with those results you'll perform the CRUD. So the order is filter -> operate -> CRUD.  

How will filtering work? I suppose it's a loop. Out of the initial objects/attributes, loop through the data and only keep data that passes.

So let's start building this tree.
##  Possibility Tree (unfinished)
### TOP LEVEL
- Object
- Attribute
- Data

### Object
- create
- read (gets metadata like attributes and types and number of rows)
- update
- delete

### Attributes
- create (objName, attrName) [metadata]
- read (objName, attrName) [metadata]
- update (objName, attrName, newName) [metadata]

## Operations By Type
#### UUID
- UUID: Regenerate?
#### Integer
- Integer: +, -, /, *, %, average, mean, median, mode, 
- Boolean: <, >, ==, !=
#### Boolean
- Boolean: true or false (just uses value)
#### String
- Boolean: Contains, Equals

## Filters
Basically use boolean operators

But then there's the creation of a row or manipulation of a row. We might need a "get relevant values" function that uses a filter if it's a filter but will essentially always be treated as a list even if it's one item in the list. 


So, how does this work in the language? Let's try with a pseudo language.
#### Scenarios
- create a new attribute that is the average of two other attributes
    - but what if we want it to automatically populate when new rows are added?? are we getting ahead of ourselves?
- get the average of an attribute
- an automatic attribute that is the median of some attributes within an object


There are automatic rows that calculate row-wise.  
There are Read calculations that calculate column-wise, but you could have a row wise calculation within a column wise Read-calculation. Somehow I need to make a list of all the things we can do, and what things can live inside other things. That's the list we started above. But let's focus on the data section.

## Data Actions
- #### Create
    - input
        - object, attribute, value
        - row-wise filter
    - output
        - true or false (success or fail)
- #### Read
    - input
        - object, attribute
        - column-wise filter
    - output
        - single value
        - column(s) of values
- #### Update
    - input
        - object, attribute, value
        - row-wise operation
    - output
        - true or false (success or fail)
        - column-wise filter
- #### Delete
    - input 
        - object, attribute
        - column-wise filter
    - output
        - true or false (success or fail)



- #### Column-Wise Operation
    - input: column of values
    - output: single value of same type

- #### Row-Wise Opertation
    - input: attribute columns with common type, a single operation for them
    - output: single value of any type



