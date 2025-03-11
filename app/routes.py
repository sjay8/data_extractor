from flask import Flask, Blueprint, jsonify, request
import csv
import json
from io import StringIO
import spacy
from .mysql_connect import get_mysql_connection
from .firebase_connect import get_firebase_connection
from .mongodb_connect import get_mongodb_connection
import re

routes = Blueprint('routes', __name__)
app = Flask(__name__)

# Initialize Firebase
firebase_db = get_firebase_connection()

# Initialize MongoDB
mongo_client = get_mongodb_connection()
db_name = "chatdb"

# Load the Spacy language model
nlp = spacy.load("en_core_web_sm")

# Dictionaries
verb_dict = {
    "find": "SELECT",
    "show": "SELECT",
    "list": "SELECT",
    "count": "COUNT",
    "add": "INSERT",
    "insert": "INSERT",
    "delete": "DELETE",
    "remove": "DELETE",
    "update": "UPDATE",
    "create": "CREATE",
    "drop": "DROP",
    "alter": "ALTER",
    "join": "JOIN",
    "merge": "JOIN"
}

prep_dict = {
    "by": "GROUP BY",
    "in": "WHERE",
    "where": "WHERE",
    "of": "FROM",
    "from": "FROM",
    "on": "ON",
    "into": "INTO",
    "as": "AS",
    "equals": "=",
    "is": "=",
    "are": "=",
    "not": "!=",
    "greater": ">",
    "less": "<",
    "equal": "=",
    "and": "AND",
    "or": "OR",
    "between": "BETWEEN",
    "like": "LIKE",
    "total": "SUM",
    "average": "AVG",
    "minimum": "MIN",
    "maximum": "MAX",
    "count": "COUNT",
    "sum": "SUM",
    "avg": "AVG",
    "distinct": "DISTINCT",
    "unique": "DISTINCT",
    "percent": "PERCENT",
    "ratio": "RATIO"
}

def parse_csv(file):
    """
    Parse CSV file and return rows as a list of dictionaries.
    """
    csv_data = StringIO(file.stream.read().decode('utf-8'))
    csv_reader = csv.DictReader(csv_data)
    return list(csv_reader)

def parse_json(file):
    """
    Parse JSON file and return data as a list of dictionaries.
    """
    json_data = json.load(file.stream)
    if isinstance(json_data, list):
        return json_data
    elif isinstance(json_data, dict):
        return [json_data]
    else:
        raise ValueError("Invalid JSON format: must be a list or object.")
@routes.route('/analyze', methods=['POST'])
def analyze_text():
    """
    Analyze text and generate SQL queries based on parts of speech and dictionaries.
    """
    try:
        # Get input text from the request
        data = request.json
        text = data.get('text', '')

        if not text:
            return jsonify({"error": "No text provided"}), 400

        # Process the text with spaCy
        doc = nlp(text)

        # Initialize query components
        query = []
        where_clause = []
        group_by_columns = []  # Collect columns for GROUP BY
        last_metric = None
        in_where = False
        in_group_by = False  # Track if we are in GROUP BY context

        for token in doc:
            word = token.text.lower()

            # Check if the token is a verb and in verb_dict
            if token.pos_ == "VERB" and word in verb_dict:
                query.append(verb_dict[word])

            # Check if the token is in prep_dict
            elif word in prep_dict:
                if word == "where":
                    in_where = True
                    query.append(prep_dict[word])
                elif word == "by" and "GROUP" in query:
                    in_group_by = True  # Start GROUP BY context
                    query.append(prep_dict[word])  # Add "BY"
                elif word in ["total", "sum", "average", "minimum", "maximum", "count"]:
                    last_metric = prep_dict[word]
                elif in_where:
                    where_clause.append(prep_dict[word])
                else:
                    query.append(prep_dict[word])

            # Handle nouns and proper nouns
            elif token.pos_ in ["NOUN", "PROPN"]:
                if in_group_by:  # Add columns to GROUP BY
                    group_by_columns.append(token.text.upper())
                elif last_metric:  # Add aggregate functions like AVG, SUM
                    query.append(f"{last_metric}({token.text.upper()})")
                    last_metric = None
                elif in_where:
                    where_clause.append(token.text.upper())
                else:
                    query.append(token.text.upper())

            # Handle numbers
            elif token.pos_ == "NUM":
                if in_where:
                    where_clause.append(token.text)
                else:
                    query.append(token.text)

        # Append GROUP BY columns to the query if present
        if group_by_columns:
            query.append("GROUP BY " + ", ".join(group_by_columns))

        # Append WHERE clause to the query if present
        if where_clause:
            query.append(" ".join(where_clause))

        # Join query components to form final query
        final_query = " ".join(query)
        return jsonify({"query": final_query}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# MongoDB-related routes
@routes.route('/mongodb/create', methods=['POST'])
def mongodb_create_from_file():
    """
    Upload data from a CSV or JSON file to a specified MongoDB collection.
    """
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400

        # Get collection name from request
        collection_name = request.form.get('collection_name')
        if not collection_name:
            return jsonify({"error": "No collection name provided"}), 400

        # Determine file type (CSV or JSON) and parse data
        if file.filename.endswith('.csv'):
            data = parse_csv(file)
        elif file.filename.endswith('.json'):
            data = parse_json(file)
        else:
            return jsonify({"error": "Unsupported file type. Please upload a CSV or JSON file."}), 400

        if not data:
            return jsonify({"error": "Empty file or invalid data."}), 400

        # Get MongoDB connection
        if mongo_client is None:
            return jsonify({"error": "Could not connect to MongoDB"}), 500

        # Access the database and collection
        db = mongo_client[db_name]
        collection = db[collection_name]

        # Insert data into the MongoDB collection
        collection.insert_many(data)
        return jsonify({"message": f"Data successfully uploaded to MongoDB collection '{collection_name}'"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500



@routes.route('/mongodb/read', methods=['POST'])
def mongodb_read():
    """
    Query MongoDB. Supports raw MongoDB queries and MySQL-like queries.
    """
    try:
        data = request.json
        mysql_query = data.get('mysql_query')
        collection_name = data.get('collection')
        mongo_query = data.get('query', {})
        projection = data.get('projection', {})
        limit = data.get('limit', None)
        sort = data.get('sort', None)

        if mysql_query:
            translated_query = mongo_from_sql(mysql_query)
            collection_name = translated_query["collection"]
            pipeline = translated_query["pipeline"]
        elif collection_name and mongo_query:
            pipeline = []
            if mongo_query:
                pipeline.append({"$match": mongo_query})
            if projection:
                pipeline.append({"$project": projection})
            if sort:
                pipeline.append({"$sort": dict(sort)})
            if limit:
                pipeline.append({"$limit": limit})
        else:
            return jsonify({"error": "No valid query provided, check your columns!"}), 400

        # Connect to MongoDB and execute the pipeline
        if mongo_client is None:
            return jsonify({"error": "Could not connect to MongoDB"}), 500

        db = mongo_client[db_name]
        collection = db[collection_name]
        data = list(collection.aggregate(pipeline))

        # Convert ObjectId to string
        for doc in data:
            doc['_id'] = str(doc['_id'])

        if not data:
            return jsonify({"message": "No data found"}), 200

        return jsonify({"mongodb_data": data}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500



@routes.route('/mysql/test', methods=['GET'])
def mysql_test():
    """
    Test MySQL connection and return some data.
    """
    connection = get_mysql_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT DATABASE();")  # Example query
                result = cursor.fetchone()
            connection.close()
            return jsonify({'database': result[0]})
        except Exception as e:
            return jsonify({'error': str(e)})
    else:
        return jsonify({'error': 'Failed to connect to MySQL'})


@routes.route('/firebase/test', methods=['GET'])
def firebase_test():
    """
    Test Firebase connection and retrieve data from a sample node.
    """
    if firebase_db:
        try:
            ref = firebase_db.reference('hospital_db')
            data = ref.get()
            return jsonify(data)
        except Exception as e:
            return jsonify({'error': str(e)})
    else:
        return jsonify({'error': 'Failed to connect to Firebase'})


@routes.route('/sql/create', methods=['POST'])
def sql_create_from_file():
    """
    Create or modify a MySQL table dynamically from a CSV or JSON file.
    """
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400

        # Extract the table name from the file name (without extension)
        table_name = file.filename.rsplit('.', 1)[0]

        # Determine file type (CSV or JSON) based on the extension
        if file.filename.endswith('.csv'):
            data = parse_csv(file)
        elif file.filename.endswith('.json'):
            data = parse_json(file)
        else:
            return jsonify({"error": "Unsupported file type. Please upload a CSV or JSON file."}), 400

        if not data:
            return jsonify({"error": "Empty file or invalid data."}), 400

        column_names = data[0].keys()
        mysql_conn = get_mysql_connection()

        # Dynamically create or alter table to match the file structure
        create_or_alter_table(mysql_conn, column_names, table_name)

        # Insert rows into MySQL
        results = []
        cursor = mysql_conn.cursor()
        for row in data:
            columns_placeholder = ', '.join(column_names)
            values_placeholder = ', '.join(['%s'] * len(column_names))
            query = f"INSERT INTO {table_name} ({columns_placeholder}) VALUES ({values_placeholder})"
            cursor.execute(query, tuple(row[col] for col in column_names))
            mysql_conn.commit()

            results.append({"mysql_id": cursor.lastrowid})

        return jsonify({"message": f"Data inserted successfully into table '{table_name}'!", "results": results}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@routes.route('/firebase/create', methods=['POST'])
def firebase_create_from_file():
    """
    Upload data from CSV or JSON file to Firebase Realtime Database, 
    storing it under a path derived from the file name.
    """
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400

        # Extract the file name without extension to use as the Firebase path
        file_name = file.filename.rsplit('.', 1)[0]

        # Determine file type (CSV or JSON) based on the extension
        if file.filename.endswith('.csv'):
            data = parse_csv(file)
        elif file.filename.endswith('.json'):
            data = parse_json(file)
        else:
            return jsonify({"error": "Unsupported file type. Please upload a CSV or JSON file."}), 400

        if not data:
            return jsonify({"error": "Empty file or invalid data."}), 400

        # Get Firebase connection
        firebase_conn = get_firebase_connection()
        if firebase_conn is None:
            return jsonify({"error": "Could not connect to Firebase"}), 500

        # Specify the path in Realtime Database using the file name
        ref = firebase_conn.reference(file_name)

        # Push rows to the database
        for row in data:
            ref.push(row)

        return jsonify({"message": f"File data successfully uploaded to Firebase under '{file_name}'"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@routes.route('/sql/read', methods=['POST'])
def sql_read():
    """
    Execute a specific query on MySQL and return results.
    """
    try:
        # Get the query from the request body
        data = request.json
        query = data.get('query', '')

        if not query:
            return jsonify({"error": "No query provided"}), 400

        mysql_conn = get_mysql_connection()
        cursor = mysql_conn.cursor()

        # Execute the query
        cursor.execute(query)
        rows = cursor.fetchall()

        # Extract column names from the cursor description
        columns = [desc[0] for desc in cursor.description]

        # Convert rows to a list of dictionaries
        results = [dict(zip(columns, row)) for row in rows]
        return jsonify({"mysql_data": results}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@routes.route('/firebase/read', methods=['POST'])
def firebase_read():
    """
    Read all data from a specific Firebase path based on the provided file name.
    """
    try:
        # Get Firebase connection
        firebase_conn = get_firebase_connection()
        if firebase_conn is None:
            return jsonify({"error": "Could not connect to Firebase"}), 500

        # Get the file name from the request body
        data = request.json
        file_name = data.get('file_name', '')

        if not file_name:
            return jsonify({"error": "No file name provided"}), 400

        # Reference the Firebase path using the file name
        ref = firebase_conn.reference(file_name)

        # Get all data from the specified node
        data = ref.get()

        if not data:
            return jsonify({"message": f"No data found under '{file_name}'"}), 200

        # Return the data
        return jsonify({"firebase_data": data}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500




@routes.route('/tables-info', methods=['GET'])
def get_tables_info():
    """
    Retrieve table and column information from MySQL and MongoDB.
    Includes the first two rows of data for each table.
    """
    try:
        # Fetch MySQL table information
        sql_tables = get_mysql_tables()

        # Fetch MongoDB collection information
        mongo_collections = get_mongodb_collections()

        return jsonify({
            "sqlTables": sql_tables,
            "mongoCollections": mongo_collections
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def get_mysql_tables():
    """
    Get MySQL table information including table name, columns, and first two rows.
    """
    connection = get_mysql_connection()
    tables_info = []

    try:
        with connection.cursor() as cursor:
            # Get list of tables
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]

            for table in tables:
                # Get column names
                cursor.execute(f"DESCRIBE {table}")
                columns = [col[0] for col in cursor.fetchall()]

                # Get first two rows of data
                cursor.execute(f"SELECT * FROM {table} LIMIT 2")
                rows = cursor.fetchall()

                tables_info.append({
                    "name": table,
                    "columns": columns,
                    "rows": [dict(zip(columns, row)) for row in rows]
                })

    except Exception as e:
        raise Exception(f"MySQL Error: {str(e)}")

    finally:
        connection.close()

    return tables_info


def get_mongodb_collections():
    """
    Get MongoDB collection information including collection name, fields, and first two rows.
    """
    if mongo_client is None:
        raise Exception("Could not connect to MongoDB")

    db = mongo_client[db_name]
    collections_info = []

    try:
        # Get list of collections
        collections = db.list_collection_names()

        for collection_name in collections:
            collection = db[collection_name]

            # Get first two rows of data
            rows = list(collection.find().limit(2))
            
            # Convert ObjectId to string
            for row in rows:
                if "_id" in row:
                    row["_id"] = str(row["_id"])
            
            fields = list(rows[0].keys()) if rows else []

            collections_info.append({
                "name": collection_name,
                "columns": fields,
                "rows": rows
            })

    except Exception as e:
        raise Exception(f"MongoDB Error: {str(e)}")

    return collections_info




def mongo_from_sql(mysql_query):
    """
    Translates a MySQL query to a MongoDB aggregation pipeline.
    Supports SELECT, WHERE, JOIN, ORDER BY, and LIMIT.
    """
    import re

    # Updated Regex pattern to parse MySQL queries
    pattern = re.compile(
        r"SELECT\s+(?P<select>\*|[\w\.,\s]+)\s+FROM\s+(?P<table1>\w+)"
        r"(\s+AS\s+(?P<alias1>\w+))?"
        r"(\s+JOIN\s+(?P<table2>\w+)\s+ON\s+(?P<join_condition>[^\s]+(?:\s+=\s+[^\s]+)))?"
        r"(\s+WHERE\s+(?P<where>.+?)(?=\s+ORDER\s+BY|\s+LIMIT|$))?"
        r"(\s+ORDER\s+BY\s+(?P<order_by>[^LIMIT]+))?"
        r"(\s+LIMIT\s+(?P<limit>\d+))?",
        re.IGNORECASE | re.DOTALL
    )

    match = pattern.match(mysql_query.strip())
    if not match:
        raise ValueError("Invalid or unsupported MySQL query format.")

    # Extract query parts
    query_parts = match.groupdict()

    # Debugging: Print extracted query parts
    print("\n--- Query Parts ---")
    for key, value in query_parts.items():
        print(f"{key}: {value}")

    table1 = query_parts["table1"]
    table2 = query_parts.get("table2")
    join_condition = query_parts.get("join_condition")
    select_fields = query_parts["select"]
    where_clause = query_parts.get("where")
    order_by_clause = query_parts.get("order_by")
    limit = query_parts.get("limit")

    pipeline = []

    # Handle JOIN
    if table2 and join_condition:
        join_parts = join_condition.split("=")
        if len(join_parts) != 2:
            raise ValueError("JOIN condition is too complex or improperly formatted.")
        left_field, right_field = [part.strip() for part in join_parts]
        left_table, left_column = left_field.split(".")
        right_table, right_column = right_field.split(".")
        pipeline.append({
            "$lookup": {
                "from": table2,
                "localField": left_column,  # Use the column name from table1
                "foreignField": right_column,  # Use the column name from table2
                "as": f"{table2}_joined"
            }
        })

    # Handle SELECT fields
    if select_fields.strip() == "*":
        pass  # MongoDB's default behavior includes all fields
    else:
        fields = [field.strip() for field in select_fields.split(",")]
        mongo_projection = {}
        for field in fields:
            if "." in field:
                table, column = field.split(".")
                if table == table2:
                    mongo_projection[f"{table2}_joined.{column}"] = 1
                else:
                    mongo_projection[column] = 1
            else:
                mongo_projection[field] = 1
        pipeline.append({"$project": mongo_projection})

    # Handle WHERE clause
    if where_clause:
        mongo_query = mongo_where_clause(where_clause.strip())
        pipeline.append({"$match": mongo_query})

    # Handle ORDER BY clause
    if order_by_clause:
        mongo_sort = []
        for clause in order_by_clause.strip().split(","):
            field, order = clause.strip().split()
            mongo_sort.append((field.split(".")[-1], 1 if order.upper() == "ASC" else -1))
        pipeline.append({"$sort": dict(mongo_sort)})

    # Handle LIMIT clause
    if limit:
        mongo_limit = int(limit)
        pipeline.append({"$limit": mongo_limit})

    # Debugging: Log the generated pipeline
    print("\n--- Generated Pipeline ---")
    print(pipeline)

    return {
        "collection": table1,
        "pipeline": pipeline
    }


def mongo_where_clause(where_clause):
    """
    Translates a MySQL WHERE clause to a MongoDB query object.
    Handles simple conditions and logical operators.
    """
    operators = {
        "=": "$eq",
        "!=": "$ne",
        ">": "$gt",
        ">=": "$gte",
        "<": "$lt",
        "<=": "$lte",
    }
    
    logical_operators = {
        "AND": "$and",
        "OR": "$or",
    }

    # Split the WHERE clause into tokens, preserving logical operators
    tokens = re.split(r"(\s+AND\s+|\s+OR\s+)", where_clause, flags=re.IGNORECASE)

    conditions = []
    logical_operator = None

    for token in tokens:
        token = token.strip()
        if token.upper() in logical_operators:  # Logical operators (AND/OR)
            logical_operator = logical_operators[token.upper()]
        else:
            # Match simple conditions
            for operator, mongo_op in operators.items():
                if operator in token:
                    field, value = token.split(operator, 1)
                    field = field.strip()
                    value = value.strip().strip("'").strip('"')  # Remove quotes
                    value = int(value) if value.isdigit() else value  # Convert numbers
                    conditions.append({field: {mongo_op: value}})
                    break
            else:
                raise ValueError(f"Unsupported condition: {token}")

    # Combine conditions with the logical operator
    if logical_operator and len(conditions) > 1:
        return {logical_operator: conditions}
    elif len(conditions) == 1:
        return conditions[0]
    else:
        raise ValueError("Invalid WHERE clause")


def create_or_alter_table(mysql_conn, column_names, table_name):
    """
    Create or alter a MySQL table to match the file structure.
    """
    cursor = mysql_conn.cursor()

    # Check if the table exists
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{table_name}'
    """)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        # Create table
        columns_definition = ', '.join([f"`{col}` TEXT" for col in column_names])
        query = f"""
            CREATE TABLE {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                {columns_definition}
            );
        """
        cursor.execute(query)
    else:
        # Alter table to add missing columns
        existing_columns = set()
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """)
        for row in cursor.fetchall():
            existing_columns.add(row[0])

        for col in column_names:
            if col not in existing_columns:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN `{col}` TEXT;")

    mysql_conn.commit()


app.register_blueprint(routes)

if __name__ == '__main__':
    app.run(debug=True)
