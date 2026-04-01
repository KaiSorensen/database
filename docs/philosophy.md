# Philosophy of Data

Regarding data, there is a fundamental tradeoff between three variables: space, time, and accuracy. If you lock accuracy at 100%, there is still a fundamental tradeoff between space and time.

What is data? Data is information that is in a pattern; it is useful. The universe is a blob of information, but data information that is useful and handlable. There is a way that it is, and a way that it isn't: a distinction.

A database distinguishes information as data. The structure of the its distinctions (schema) is fitted to the problem it is addressing (more specifically: the questions to be asked of it). Distinctions are made **by** the structure, not **after** the structure.

You could, theoretically, store the answer to questions in advance. That is to exchange space for time. There is no perfect formula for calculating the optimal space/time tradeoff as it depends on many variables whose value may change with time in the real world. For now, it may be worth tracking space and time for the sake of preparing to strike that balance when a valued variable emerges.

Computers are built on binary operations. Therefore, the fastest retrieval operation can only be log^2 time. You could, however, expend space to save time in retrieval through trickery such as hashing, in which the hashes map to addresses.

Often we take for granted that data has structure, yet it is the structuring of data that is the most sensitive and difficult aspect of databases.