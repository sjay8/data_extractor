import React, { useState } from "react";
import "./App.css";
import executeQuery from './firebase_query'; // Import executeQuery

function App() {
  const [messages, setMessages] = useState([]);
  const [inputText, setInputText] = useState("");
  const [selectedFile, setSelectedFile] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [dataSource, setDataSource] = useState("mysql"); // Default MySQL


  // Predefined responses
  const responses = {
    "hi": "Hi! How can I help you today?",
    "hello": "Hello! How can I assist you today?",
    Hey: "Hey! How can I help?",
    "Whats up": "Nothing much, how can I help you today?",
    "Good morning": "Good morning! What can I do for you today?",
    "Good afternoon": "Good afternoon! How can I assist?",
    "Good evening": "Good evening! How may I help?",
    "whats your name": "My name is ChatDB.",
    "what are you":
      "I’m ChatDB, your assistant for querying databases and analyzing data. I can help you with SQL queries, Firebase data, and much more!",
    "who are you":
      "I’m ChatDB, your assistant for querying databases and analyzing data. I can help you with SQL queries, Firebase data, and much more!",
    "what do you do":
      "I help with querying databases, analyzing data, and explaining concepts in natural language.",
    "why are you here":
      "I’m here to assist you with database tasks, answer questions, and make your data analysis simpler!",
    "bye": "Goodbye! Have a great day!",
    "See you": "See you later! Let me know if you need help again.",
    Goodbye: "Goodbye! Feel free to come back if you have more questions.",
    "Thanks, bye": "You’re welcome! Take care!",
    "Catch you later": "Catch you later! Have a good one!",
  };


  const exampleQueries = {
    "select": {
        "example_1": {
            "query": "SELECT * FROM {table_name};",
            "explanation": "Retrieve all columns and rows from the table named '{table_name}'."
        },
        "example_2": {
            "query": "SELECT {column1}, {column2} FROM {table_name};",
            "explanation": "Retrieve the '{column1}' and '{column2}' columns from the table named '{table_name}'."
        },
        "example_3": {
            "query": "SELECT {column1} AS C_1, {column2} AS C_2 FROM {table_name};",
            "explanation": "Retrieve '{column1}' and '{column2}' from the table '{table_name}', displaying them as 'C_1' and 'C_2' respectively."
        },
        "example_4": {
            "query": "SELECT DISTINCT {column1} FROM {table_name};",
            "explanation": "Retrieve unique values from '{column1}' in '{table_name}'."
        },
        "example_5": {
            "query": "SELECT {column1}, COUNT({column2}) AS count_value FROM {table_name} GROUP BY {column1};",
            "explanation": "Retrieve '{column1}' and the count of '{column2}' grouped by '{column1}' from '{table_name}'."
        }
    },
    "where": {
        "example_1": {
            "query": "SELECT * FROM {table_name} WHERE {column1} = '{value_1}';",
            "explanation": "Retrieve all rows from the table '{table_name}' where '{column1}' has the value '{value_1}'."
        },
        "example_2": {
            "query": "SELECT * FROM {table_name} WHERE {column1} LIKE '{prefix}%';",
            "explanation": "Retrieve rows where '{column1}' starts with '{prefix}'."
        },
        "example_3": {
            "query": "SELECT * FROM {table_name} WHERE {column1} BETWEEN {value_1} AND {value_2};",
            "explanation": "Retrieve rows where '{column1}' is between {value_1} and {value_2}."
        },
        "example_4": {
            "query": "SELECT * FROM {table_name} WHERE {column1} IS NULL;",
            "explanation": "Retrieve rows where '{column1}' has no value (NULL)."
        },
        "example_5": {
            "query": "SELECT * FROM {table_name} WHERE {column1} IN ('{value_1}', '{value_2}');",
            "explanation": "Retrieve rows where '{column1}' matches any value in the specified list of ('{value_1}', '{value_2}')."
        }
    },
    "group_by": {
        "example_1": {
            "query": "SELECT {column1}, COUNT(*) FROM {table_name} GROUP BY {column1};",
            "explanation": "Group rows in the table '{table_name}' by the '{column1}' value and return each unique value along with the count of rows for that value."
        },
        "example_2": {
            "query": "SELECT {column1}, AVG({column2}) FROM {table_name} GROUP BY {column1};",
            "explanation": "Group rows in '{table_name}' by '{column1}' and calculate the average of '{column2}' for each group."
        },
        "example_3": {
            "query": "SELECT {column1}, MAX({column2}) FROM {table_name} GROUP BY {column1};",
            "explanation": "Retrieve '{column1}' and the maximum value of '{column2}' grouped by '{column1}' in '{table_name}'."
        },
        "example_4": {
            "query": "SELECT {column1}, MIN({column2}) FROM {table_name} GROUP BY {column1};",
            "explanation": "Retrieve '{column1}' and the minimum value of '{column2}' grouped by '{column1}' in '{table_name}'."
        },
        "example_5": {
            "query": "SELECT {column1}, SUM({column2}) FROM {table_name} GROUP BY {column1} HAVING SUM({column2}) > 100;",
            "explanation": "Group rows in '{table_name}' by '{column1}', calculate the sum of '{column2}' for each group, and retrieve only groups where the sum is greater than 100."
        }
    },
    "having": {
        "example_1": {
            "query": "SELECT {column1}, COUNT(*) FROM {table_name} GROUP BY {column1} HAVING COUNT(*) > 10;",
            "explanation": "Retrieve groups from '{table_name}' where the count of rows in each group exceeds 10."
        },
        "example_2": {
            "query": "SELECT {column1}, SUM({column2}) FROM {table_name} GROUP BY {column1} HAVING SUM({column2}) < 100;",
            "explanation": "Retrieve groups where the sum of '{column2}' is less than 100."
        },
        "example_3": {
            "query": "SELECT {column1}, AVG({column2}) FROM {table_name} GROUP BY {column1} HAVING AVG({column2}) > 50;",
            "explanation": "Retrieve groups where the average of '{column2}' is greater than 50."
        },
        "example_4": {
            "query": "SELECT {column1}, MAX({column2}) FROM {table_name} GROUP BY {column1} HAVING MAX({column2}) = 100;",
            "explanation": "Retrieve groups where the maximum value of '{column2}' is 100."
        },
        "example_5": {
            "query": "SELECT {column1}, MIN({column2}) FROM {table_name} GROUP BY {column1} HAVING MIN({column2}) < 20;",
            "explanation": "Retrieve groups where the minimum value of '{column2}' is less than 20."
        }
    },
    "order_by": {
        "example_1": {
            "query": "SELECT * FROM {table_name} ORDER BY {column1};",
            "explanation": "Retrieve all rows from '{table_name}' and sort them by '{column1}' in ascending order."
        },
        "example_2": {
            "query": "SELECT * FROM {table_name} ORDER BY {column1} DESC;",
            "explanation": "Retrieve all rows from '{table_name}' and sort them by '{column1}' in descending order."
        },
        "example_3": {
            "query": "SELECT * FROM {table_name} ORDER BY {column1}, {column2};",
            "explanation": "Sort rows in '{table_name}' by '{column1}' and then by '{column2}' in ascending order."
        },
        "example_4": {
            "query": "SELECT * FROM {table_name} ORDER BY {column1} ASC, {column2} DESC;",
            "explanation": "Sort rows by '{column1}' in ascending order and '{column2}' in descending order."
        },
        "example_5": {
            "query": "SELECT * FROM {table_name} ORDER BY LENGTH({column1});",
            "explanation": "Sort rows by the length of the values in '{column1}'."
        }
    },
    "join": {
        "example_1": {
            "query": "SELECT {table1}.{column1}, {table2}.{column2} FROM {table1} INNER JOIN {table2} ON {table1}.{column3} = {table2}.{column3};",
            "explanation": "Combine rows from '{table1}' and '{table2}' where '{column3}' in both tables match."
        },
        "example_2": {
            "query": "SELECT {table1}.{column1}, {table2}.{column2} FROM {table1} LEFT JOIN {table2} ON {table1}.{column3} = {table2}.{column3};",
            "explanation": "Retrieve all rows from '{table1}' and matching rows from '{table2}', leaving unmatched rows from '{table1}'."
        },
        "example_3": {
            "query": "SELECT {table1}.{column1}, {table2}.{column2} FROM {table1} RIGHT JOIN {table2} ON {table1}.{column3} = {table2}.{column3};",
            "explanation": "Retrieve all rows from '{table2}' and matching rows from '{table1}', leaving unmatched rows from '{table2}'."
        },
        "example_4": {
            "query": "SELECT {table1}.{column1}, {table2}.{column2} FROM {table1} FULL OUTER JOIN {table2} ON {table1}.{column3} = {table2}.{column3};",
            "explanation": "Combine rows from both tables, retrieving all rows where matches occur or not."
        },
        "example_5": {
            "query": "SELECT {table1}.{column1}, {table2}.{column2} FROM {table1} CROSS JOIN {table2};",
            "explanation": "Combine all rows from '{table1}' with all rows from '{table2}'."
        }
    }
}













const replacePlaceholders = async (count = 5, examples = null) => {
  try {
    const res = await fetch("http://127.0.0.1:5000/tables-info");
    const data = await res.json();

    const sourceTables =
      dataSource === "mysql" ? data.sqlTables : data.mongoCollections;

    if (!sourceTables.length) {
      throw new Error(`No tables found in ${dataSource}`);
    }

    const getRandomElement = (array) =>
      array[Math.floor(Math.random() * array.length)];

    const getExampleValues = (rows, column) => {
      if (!rows.length || !column) return [];
      return rows
        .map((row) => row[column])
        .filter((value) => value !== undefined && value !== null);
    };

    const isNumericColumn = (values) =>
      values.every((value) => !isNaN(value) && typeof value !== "string");

    const isStringColumn = (values) =>
      values.every((value) => typeof value === "string");

    const selectedExamples =
      examples || Object.values(exampleQueries).flatMap((group) =>
        Object.values(group)
      ).slice(0, count);

    const replacedExamples = await Promise.all(
      selectedExamples.map(async (example) => {
        const randomTable1 = getRandomElement(sourceTables);
        const randomColumns1 = randomTable1.columns || [];
        const randomColumn1 = getRandomElement(randomColumns1) || "column1";
        const randomColumn2 = getRandomElement(randomColumns1.filter(c => c !== randomColumn1)) || "column2";

        const valuesForColumn1 = getExampleValues(
          randomTable1.rows,
          randomColumn1
        );

        const isNumeric = isNumericColumn(valuesForColumn1);
        const isString = isStringColumn(valuesForColumn1);

        const randomValue1 = valuesForColumn1[0] || "value_1";
        const randomValue2 = valuesForColumn1[1] || randomValue1;

        const randomPrefix =
          isString && randomValue1 ? randomValue1.toString().slice(0, 3) : "prefix";

        let replacements = {
          "{table_name}": randomTable1.name || "table_name",
          "{column1}": randomColumn1,
          "{column2}": randomColumn2,
          "{value_1}": example.query.includes("IN") ||
            example.query.includes("BETWEEN")
            ? randomValue1
            : randomValue1,
          "{value_2}": example.query.includes("BETWEEN")
            ? randomValue2
            : randomValue2,
          "{prefix}": example.query.includes("LIKE") && isString
            ? randomPrefix
            : "prefix",
        };

        // Specific logic for JOIN queries
        if (example.query.includes("JOIN")) {
          const randomTable2 = getRandomElement(
            sourceTables.filter((table) => table.name !== randomTable1.name)
          );

          const commonColumns = findJoinColumns(randomTable1, randomTable2);

          if (commonColumns) {
            const joinColumn = getRandomElement(commonColumns);
            replacements["{table1}"] = randomTable1.name;
            replacements["{table2}"] = randomTable2.name;
            replacements["{column3}"] = joinColumn;
            replacements["{column2}"] = getRandomElement(
              randomTable2.columns || []
            ) || "column2";
          } else {
            throw new Error(
              `No common columns for JOIN between ${randomTable1.name} and ${randomTable2.name}`
            );
          }
        }

        const replacedQuery = replaceAllPlaceholders(example.query, replacements);
        const replacedExplanation = replaceAllPlaceholders(
          example.explanation,
          replacements
        );

        // Call backend to execute the query and get results
        const apiEndpoint =
          dataSource === "mysql"
            ? "http://127.0.0.1:5000/sql/read"
            : "http://127.0.0.1:5000/mongodb/read";

        const requestBody =
            dataSource === "mysql"
              ? { query: replacedQuery }
              : { mysql_query: replacedQuery };

              let queryResult = "";
try {
  const response = await fetch(apiEndpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(requestBody),
  });
  const result = await response.json();

  if (dataSource === "mysql" && result.mysql_data && Array.isArray(result.mysql_data)) {
    const mysqlData = result.mysql_data;

    if (mysqlData.length > 0) {
      const headers = Object.keys(mysqlData[0]);
      const rows = mysqlData.map((row) =>
        headers.map((header) => row[header] !== null ? row[header] : "").join(" | ")
      );

      // 格式化结果为表格样式字符串
      queryResult = [
        "",
        "",
        headers.join(" | "), // 表头
        "-".repeat(headers.join(" | ").length), // 分隔符
        ...rows, // 数据行
        "-".repeat(100),
        "",
      ].join("\n");
    } else {
      queryResult = ["\n\nNo data returned from the query.\n\n","-".repeat(100),];
    }
  } else {
    queryResult = [`\n\nResults:\n\n${JSON.stringify(result, null, 2)}\n\n`, "-".repeat(100),];
  }
} catch (error) {
  console.error("Error fetching query result:", error);
  queryResult = "\n\nError fetching results from backend.\n\n";
}




      
              return {
                query: replacedQuery,
                explanation: `${replacedExplanation}\n\nResults:\n${queryResult}`,
              };
            })
          );
      
          return replacedExamples;
        } catch (error) {
          console.error("Error replacing placeholders:", error);
          throw error;
        }
      };
      

const replaceAllPlaceholders = (text, replacements) => {
  let replacedText = text;
  for (const [placeholder, value] of Object.entries(replacements)) {
    replacedText = replacedText.split(placeholder).join(value);
  }
  return replacedText;
};

const findJoinColumns = (table1, table2) => {
  const columns1 = new Set(table1.columns || []);
  const columns2 = new Set(table2.columns || []);
  const joinableColumns = [...columns1].filter((col) => columns2.has(col));
  return joinableColumns.length ? joinableColumns : null;
};


const formatWithLineBreaks = (text) => {
  return text.split("\n").map((line, idx) => (
    <pre key={idx} style={{ margin: 0, whiteSpace: "pre-wrap" }}>
      {line}
    </pre>
  ));
};


  const handleTextChange = (event) => {
    setInputText(event.target.value);
  };

  const handleSend = async () => {
  if (!inputText) {
    alert("Please enter a message.");
    return;
  }

  // Add user's text message to the chat
  setMessages((prevMessages) => [
    ...prevMessages,
    { sender: "user", text: inputText },
  ]);

  
  // Predefined responses
  const userMessage = inputText.trim().toLowerCase();
  if (responses[userMessage]) {
    setMessages((prevMessages) => [
      ...prevMessages,
      { sender: "bot", text: responses[userMessage] },
    ]);
    setInputText("");
    return;
  }



 // 检测 `show columns name in table`
 if (userMessage.startsWith("show columns name in table")) {
  const tableName = userMessage.split(" ").pop(); // 获取表名
  try {
    // 调用 tables-info 接口
    const res = await fetch("http://127.0.0.1:5000/tables-info", {
      method: "GET",
    });
    const data = await res.json();

    // 查找表名
    const table = data.sqlTables.find(
      (t) => t.name.toLowerCase() === tableName.toLowerCase()
    );

    if (table) {
      // 返回表的列名
      setMessages((prevMessages) => [
        ...prevMessages,
        {
          sender: "bot",
          text: `Columns in table '${tableName}':\n${table.columns.join(", ")}`,
        },
      ]);
    } else {
      // 表不存在
      setMessages((prevMessages) => [
        ...prevMessages,
        {
          sender: "bot",
          text: `Table '${tableName}' not found.`,
        },
      ]);
    }
  } catch (error) {
    console.error("Error fetching tables info:", error);
    setMessages((prevMessages) => [
      ...prevMessages,
      { sender: "bot", text: "Error fetching table information." },
    ]);
  }
  setInputText("");
  return;
}













  // 检测 "example"
  if (userMessage.includes("example")) {
    const words = userMessage.split(" ");
    const exampleIndex = words.indexOf("example");

    // 检测 "key example"
    if (exampleIndex > 0) {
      const key = words[exampleIndex - 1];
      if (exampleQueries[key]) {
        try {
          const examplesForKey = Object.values(exampleQueries[key]);
          const replacedExamples = await replacePlaceholders(
            examplesForKey.length,
            examplesForKey
          );

          const exampleTexts = replacedExamples
            .map(
              (example, idx) =>
                `\n${idx + 1}. ${example.query}\nExplanation:\n${example.explanation}\n\n`
            )
            .join("");


          setMessages((prevMessages) => [
            ...prevMessages,
            { sender: "bot", text: `Examples for '${key}':\n\n${exampleTexts}` },
          ]);
        } catch (error) {
          console.error("Error generating examples for key:", error);
          setMessages((prevMessages) => [
            ...prevMessages,
            {
              sender: "bot",
              text: `Error generating examples for '${key}'.`,
            },
          ]);
        }
        setInputText("");
        return;
      }
    }

    // 处理没有 key 的 "example"
    try {
      const replacedExamples = await replacePlaceholders(5);

      const exampleTexts = replacedExamples
        .map(
          (example, idx) =>
            `${idx + 1}. ${example.query}\nExplanation:\n${example.explanation}`
        )
        .join("\n\n");

      setMessages((prevMessages) => [
        ...prevMessages,
        { sender: "bot", text: `Here are 5 random examples:\n\n${exampleTexts}` },
      ]);
    } catch (error) {
      console.error("Error generating examples:", error);
      setMessages((prevMessages) => [
        ...prevMessages,
        { sender: "bot", text: "Error generating examples." },
      ]);
    }
    setInputText("");
    return;
  }




    // Fallback to backend API call
    try {
      // 调用 /analyze 接口获取查询
      const res = await fetch("http://127.0.0.1:5000/analyze", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ text: inputText }),
      });
    
      const data = await res.json();
    
      if (data.query) {
        // 根据数据源决定使用的后端 API 和请求体
        const apiEndpoint =
          dataSource === "mysql"
            ? "http://127.0.0.1:5000/sql/read"
            : "http://127.0.0.1:5000/mongodb/read";
    
        const requestBody =
          dataSource === "mysql"
            ? { query: `${data.query.trim()};`} // MySQL 使用 query 字段
            : { mongo_query: `${data.query.trim()};` }; // MongoDB 使用 mongo_query 字段
    
        // 向后端发送查询请求
        const queryRes = await fetch(apiEndpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(requestBody),
        });
    
        const queryResult = await queryRes.json();
    
        // 处理结果
        let resultText = "";
        if (dataSource === "mysql" && queryResult.mysql_data && Array.isArray(queryResult.mysql_data)) {
          const mysqlData = queryResult.mysql_data;
    
          if (mysqlData.length > 0) {
            const headers = Object.keys(mysqlData[0]);
            const rows = mysqlData.map((row) =>
              headers.map((header) => row[header] !== null ? row[header] : "").join(" | ")
            );
    
            resultText = [
              headers.join(" | "), // 表头
              "-".repeat(headers.join(" | ").length), // 分隔符
              ...rows, // 数据行
              "-".repeat(100), // 结果后的空行
            ].join("\n");
          } else {
            resultText = "No data returned from the query.";
          }
        } else if (dataSource === "mongodb") {
          resultText = JSON.stringify(queryResult, null, 2);
        } else {
          resultText = "format is wrong! Check your column name.";
        }
    
        // 显示查询和结果
        setMessages((prevMessages) => [
          ...prevMessages,
          {
            sender: "bot",
            text: `The query:\n${data.query}\n\nResult:\n${resultText}`,
          },
        ]);
      } else {
        setMessages((prevMessages) => [
          ...prevMessages,
          { sender: "bot", text: "Analyze did not return a query." },
        ]);
      }
    } catch (error) {
      console.error("Error:", error);
      setMessages((prevMessages) => [
        ...prevMessages,
        { sender: "bot", text: "Error processing your request." },
      ]);
    }
    
  
    setInputText(""); // 清空输入框
  };













  const handleFileChange = (event) => {
    setSelectedFile(event.target.files[0]);
  };

  const handleUpload = async (target) => {
    if (!selectedFile) {
      alert("Please select a file to upload.");
      return;
    }

    const formData = new FormData();
    formData.append("file", selectedFile);

    try {
      const endpoint =
        target === "mysql"
          ? "http://127.0.0.1:5000/sql/create"
          : target === "firebase"
          ? "http://127.0.0.1:5000/firebase/create"
          : "http://127.0.0.1:5000/mongodb/create"; // MongoDB endpoint
  
      // For MongoDB, add a "collection_name" form field
      if (target === "mongodb") {
        const collectionName = prompt("Enter MongoDB collection name:");
        if (!collectionName) {
          alert("Collection name is required for MongoDB.");
          return;
        }
        formData.append("collection_name", collectionName);
      }
  
      const res = await fetch(endpoint, {
        method: "POST",
        body: formData,
      });
  
      const data = await res.json();
  
      // Add backend response to the chat
      setMessages((prevMessages) => [
        ...prevMessages,
        {
          sender: "bot",
          text: data.message || "Unexpected response from server.",
        },
      ]);
    } catch (error) {
      console.error("Upload error:", error);
      setMessages((prevMessages) => [
        ...prevMessages,
        { sender: "bot", text: "Error uploading file." },
      ]);
    }
  
    setSelectedFile(null);
    setIsModalOpen(false); // Close modal
  };

  return (
    <div className="App">
      <div className="chat-wrapper">
        <header className="chat-header">
          <h1>ChatDB</h1>
          <p>Your AI-powered assistant for databases</p>
        </header>
        <div className="chat-content">
          {messages.map((msg, index) => (
            <div
              key={index}
              className={`message ${
                msg.sender === "user" ? "user-message" : "bot-message"
              }`}
            >
              {msg.sender === "bot" ? formatWithLineBreaks(msg.text) : msg.text}
            </div>
          ))}
        </div>


        <div className="data-source-selector">
          <button
            className={`selector-button ${dataSource === "mysql" ? "active" : ""}`}
            onClick={() => setDataSource("mysql")}
          >
            MySQL
          </button>
          <button
            className={`selector-button ${dataSource === "mongodb" ? "active" : ""}`}
            onClick={() => setDataSource("mongodb")}
          >
            MongoDB
          </button>
        </div>
        
        








        <div className="chat-input-area">
          <textarea
            className="chat-input"
            placeholder="Type your message..."
            value={inputText}
            onChange={handleTextChange}
          />
          <button className="send-button" onClick={handleSend}>
            Send
          </button>
          <button
            className="file-upload-button"
            onClick={() => setIsModalOpen(true)}
          >
            Upload
          </button>
        </div>
      </div>

      {/* Modal for upload options */}
      {isModalOpen && (
        <div className="upload-modal active">
          <div className="modal-content">
            <button
              className="modal-close"
              onClick={() => setIsModalOpen(false)}
            >
              &times;
            </button>
            <h2>Upload File</h2>
            <input type="file" onChange={handleFileChange} />
              <div className="modal-buttons">
                <button
                  className="modal-button mysql"
                  onClick={() => handleUpload("mysql")}
                >
                  Upload to MySQL
                </button>
                <button
                  className="modal-button firebase"
                  onClick={() => handleUpload("firebase")}
                >
                  Firebase (no queries)
                </button>
                <button
                  className="modal-button mongodb"
                  onClick={() => handleUpload("mongodb")}
                >
                  Upload to MongoDB
                </button>
              </div>

          </div>
        </div>
      )}
    </div>
  );
}

export default App;
