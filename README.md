# gskeleton

This project utilizes utilizes Python, SQL, and Google APIs to automate manual ETLs operations done in Google Drive, Sheets, etc that individuals and small business spend precious time on.

Example config:

```
extractors:
-   name: users_and_orders
    location:
        key: {Input folder key}
        type: folder
        mime_type: application/vnd.google-apps.spreadsheet
    tables:
    -   name: users
        sheet:
            index: 1
            name: Bizz Users
            box:
                header_row: 0
                start_row: 1
    -   name: orders
        sheet:
            index: 0
            name: Bizz Orders
            box:
                header_row: 1
                start_row: 4
                start_column: 1
trasnformers:
-   sql_commant: >
        CREATE TABLE first_time_users AS
            SELECT user_id, first_name, middle_name, last_name
            FROM users
            WHERE previous_orders = 0;
-   sql_commant: >
        CREATE TABLE first_time_user_orders AS
            SELECT first_name, middle_name, last_name
            FROM orders o
            JOIN first_time_users ftu ON o.user_id = ftu.user_id;
loaders:
-   name: New_Users
    suffix_type: unix
    extension: xlsx
    template:
        key: {Users template key}
    exports:
        key: {Exports folder key}
    tables:
    -   name: first_time_users
        box:
            sheet_id: 0
-   name: New_User_Orders
    suffix_type: unix
    extension: xlsx
    template:
        key: {NewUserOrderTemplate.xlsx}
    exports:
        key: {Exports folder key}
    tables:
    -   name: first_time_user_orders
        box:
            sheet_id: 0
```
