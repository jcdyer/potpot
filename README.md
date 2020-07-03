Initial steps:

*   Create StorageManager that handles files and pages, and a pluggable (trait 
    based) buffer pool.
*   First buffer pool can just hold a single page, and always request a new
    page from disk.
