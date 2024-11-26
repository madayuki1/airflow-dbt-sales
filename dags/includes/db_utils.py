# # List of column names and their types
# example
# columns = [
#     ("id", "SERIAL PRIMARY KEY"),
#     ("name", "VARCHAR(100)"),
#     ("age", "INT"),
#     ("email", "VARCHAR(255)"),
#     ("created_at", "TIMESTAMP")
# ]

# Function to generate the CREATE TABLE SQL statement
def generate_create_table_sql(table_name, columns, schema='public'):
    # Generate the column definitions part
    column_definitions = ", ".join([f"{col[0]} {col[1]}" for col in columns])
    
    # SQL statement to create the table if it doesn't exist
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        {column_definitions}
    );
    """
    return create_table_sql

def generate_insert_table_sql(table_name, columns):
    # Generate the column definitions part
    column_definitions = ", ".join([f"{col}" for col in columns])
    placeholders =", ".join([f"%s" for col in columns])
    
    # SQL statement to create the table if it doesn't exist
    insert_table_sql = f"""
    INSERT INTO {table_name} (
        {column_definitions}
    )
    VALUE(
        {placeholders}
    );
    """
    return insert_table_sql