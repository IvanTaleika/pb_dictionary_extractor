# pb_dictionary_extractor

## Project description
A small non-commercial app that is retrieving annotated text from the PocketBook EBook pass it through a several 
words API and save enriched text in a CSV format, so it can be opened by Google Sheets / Excel.

The app was created to simplify the education process by automizing dictionary creation process.

The app was created for a **PocketBook Aqua 2** model. There are no guarantee other models use the same 
data model or even a storage technology.

## Tech stack
* The app core is Apache-Spark. This is certainly an overkill. It is just a technology I am used to.
* SQLite JDBC client, as it is the storage format for PocketBook Aqua 2 model.
* Utility / supporting libraries for HTTP request etc.
