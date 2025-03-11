function executeQuery(data, query) {
    const firebaseData = data.firebase_data;
  
    // Parse the SELECT, WHERE, GROUP BY, and ORDER BY parts
    const [mainPart, orderByPart] = query.toLowerCase().split(" order by ");
    const [selectWherePart, groupByPart] = mainPart.split(" group by ");
    const [selectPart, wherePart] = selectWherePart.split(" where ");
  
    const selectedFields = parseFields(selectPart, "select");
    const groupByFields = groupByPart ? parseFields(groupByPart) : [];
    const orderByFields = orderByPart ? orderByPart.trim().split(" ") : [];
    const conditions = wherePart ? parseConditions(wherePart) : [];
  
    // Filter data based on WHERE conditions
    const filteredData = filterData(firebaseData, conditions);
  
    let finalData;
  
    if (groupByFields.length > 0) {
      // Handle grouping
      const groupedData = groupData(filteredData, selectedFields, groupByFields);
      calculateAggregates(groupedData, selectedFields);
      finalData = Object.values(groupedData);
    } else {
      // No grouping, just select fields from filtered data
      finalData = filteredData.map((record) => {
        const selectedRecord = {};
        selectedFields.forEach((field) => {
          if (record[field] !== undefined) {
            selectedRecord[field] = isNumeric(record[field])
              ? parseFloat(record[field])
              : record[field];
          }
        });
        return selectedRecord;
      });
    }
  
    // Apply ORDER BY if specified
    if (orderByFields.length > 0) {
      finalData = sortData(finalData, orderByFields);
    }
  
    return finalData;
  }
  
  // Parse the selected fields
  function parseFields(part, prefix = "") {
    return part
      .replace(prefix, "")
      .replace("from", "")
      .trim()
      .split(",")
      .map((field) => field.trim());
  }
  
  // Parse the WHERE conditions
  function parseConditions(wherePart) {
    return wherePart.split("and").map((cond) => cond.trim());
  }
  
  // Parse a condition like "age = 20"
  function parseCondition(condition) {
    const match = condition.match(/(\w+)\s*(=|!=|>|<|>=|<=|between|in|like)\s*['"]?([\w\s,]+)['"]?/);
    if (match) {
      return [match[1].trim(), match[2].trim(), match[3].trim()];
    }
    throw new Error(`Invalid condition: ${condition}`);
  }
  
  // Filter data based on WHERE conditions
  function filterData(firebaseData, conditions) {
    return Object.values(firebaseData).filter((record) =>
      conditions.every((condition) => evaluateCondition(record, parseCondition(condition)))
    );
  }
  
  // Evaluate a condition for a record
  function evaluateCondition(record, [field, operator, value]) {
    const fieldValue = isNumeric(record[field])
      ? parseFloat(record[field])
      : record[field];
    switch (operator) {
      case "=":
        return fieldValue == value;
      case "!=":
        return fieldValue != value;
      case ">":
        return fieldValue > parseFloat(value);
      case "<":
        return fieldValue < parseFloat(value);
      case ">=":
        return fieldValue >= parseFloat(value);
      case "<=":
        return fieldValue <= parseFloat(value);
      case "between":
        const [min, max] = value.split("and").map((v) => parseFloat(v.trim()));
        return fieldValue >= min && fieldValue <= max;
      case "in":
        return value.split(",").map((v) => v.trim()).includes(fieldValue);
      case "like":
        const regex = new RegExp(value.replace(/%/g, ".*"), "i");
        return regex.test(fieldValue);
      default:
        return false;
    }
  }
  
  // Check if a value is numeric
  function isNumeric(value) {
    return !isNaN(parseFloat(value)) && isFinite(value);
  }
  
  // Group data by specified fields
  function groupData(filteredData, selectedFields, groupByFields) {
    const grouped = {};
  
    filteredData.forEach((record) => {
      const groupKey = groupByFields.map((field) => record[field]).join("-");
      if (!grouped[groupKey]) {
        grouped[groupKey] = { _groupByKey: groupKey, _count: 0 };
        groupByFields.forEach((field) => {
          grouped[groupKey][field] = record[field];
        });
      }
      grouped[groupKey]._count += 1;
  
      selectedFields.forEach((field) => {
        if (field.startsWith("sum(")) {
          const actualField = field.match(/\((.*)\)/)[1];
          grouped[groupKey][`sum(${actualField})`] =
            (grouped[groupKey][`sum(${actualField})`] || 0) +
            parseFloat(record[actualField]);
        } else if (field.startsWith("avg(")) {
          const actualField = field.match(/\((.*)\)/)[1];
          grouped[groupKey][`sum(${actualField})`] =
            (grouped[groupKey][`sum(${actualField})`] || 0) +
            parseFloat(record[actualField]);
        }
      });
    });
  
    return grouped;
  }
  
  // Calculate aggregates like AVG
  function calculateAggregates(groupedData, selectedFields) {
    Object.values(groupedData).forEach((group) => {
      selectedFields.forEach((field) => {
        if (field.startsWith("avg(")) {
          const actualField = field.match(/\((.*)\)/)[1];
          group[`avg(${actualField})`] =
            group[`sum(${actualField})`] / group._count;
        }
      });
    });
  }
  
  // Sort the data based on ORDER BY fields
  function sortData(data, orderByFields) {
    const [field, direction] = orderByFields;
    return data.sort((a, b) => {
      const aValue = isNumeric(a[field]) ? parseFloat(a[field]) : a[field];
      const bValue = isNumeric(b[field]) ? parseFloat(b[field]) : b[field];
      if (direction === "desc") {
        return bValue - aValue;
      }
      return aValue - bValue;
    });
  }
  
  export default executeQuery;
  