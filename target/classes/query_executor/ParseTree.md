What is a parse tree and how does it work?

The language parser takes the language and converts it into a parse tree.

What is a parse tree? Why is it a tree?

Well, we need to have a hierarchy of actions. Theoretically, we could have a hierarchy of all possibilities. In SQL, this tree would be extremely complicated. In our case, it's manageable. For example, are we manipulating data? We we manipulating an attribute? Are we manupulating an object? Those can't overlap or occur in the same language statement. Therefore, they could be the tree roots. If editing data, are we updating/deleting? Are we filtering? How much data are we editing? This requires explicit examination.