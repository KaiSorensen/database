Each table gets a file that is separated into fixed-size pages.

Why fixed-size pages? It guarantees that we can lookup items fast, and that the worst case scenario is having to look through the size of one page.

Metadata will determine which file and which page to look through to find a row.

- Indexing will work by hashing the exact location of an item.  
- Insertion will add to the end; deletion will add to the beginning.  
- Pages will be a fixed byte size.  
- Metadata will be a separate file. 


#### Okay so here are the layers:
- metadata files holding pointers (everything is of a fixed size)
- table files are paginated, and hold pointers & sizes
- an actual memory allocator that holds the data and works like an operating system allocator with bitmaps of free space & such, to minimize empty space during operations (should there be a full cleanup operation sometimes, or will the allocation strategy, like buddy system, avoid bad worst case scenarios?)


#### Metadata Files
- table and column names, basically a list of all the names and such
- each column is a file of "addresses"
- the "addresses" access a main db file that squishes all data together in an addressable way through a single point of allocation

