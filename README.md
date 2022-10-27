# GSkeleton

This project utilizes Python, SQL, and Google APIs to automate ETL operations in Google Sheets and Google Drive that businesses often spend precious resources doing manually.

Example ETL configuration YAML file:

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
            name: Users
            box:
                header_row: 0
                start_row: 1
    -   name: orders
        sheet:
            index: 0
            name: Orders
            box:
                header_row: 1
                start_row: 4
                start_column: 1
trasnformers:
-   sql_command: >
        CREATE TABLE first_time_users AS
            SELECT user_id, first_name, middle_name, last_name
            FROM users
            WHERE previous_orders = 0;
-   sql_command: >
        CREATE TABLE first_time_user_orders AS
            SELECT order_id, first_name, middle_name, last_name
            FROM orders o
            JOIN first_time_users ftu ON o.user_id = ftu.user_id;
loaders:
-   name: New_Users
    suffix_type: unix
    extension: xlsx
    template:
        key: {New users template key}
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
        key: {New user orders template key}
    exports:
        key: {Exports folder key}
    tables:
    -   name: first_time_user_orders
        box:
            sheet_id: 0
```
