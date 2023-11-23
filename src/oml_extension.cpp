#define DUCKDB_EXTENSION_MAIN

#include "include/oml_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <fstream>
#include <iostream>
#include <openssl/opensslv.h>
#include <regex>

namespace duckdb {

struct OMLFunctionData : public TableFunctionData {
  string file;
  vector<LogicalType> return_types;
};

struct OMLData : public GlobalTableFunctionState {
  bool finished = false;
  int linesRead = 0;
};

static void InitTable(vector<LogicalType> &return_types, vector<string> &return_names) {
  std::vector<std::tuple<std::string, LogicalType>> cols = {
      {"id", LogicalType::INTEGER},
      {"ts", LogicalType::FLOAT},
      {"power", LogicalType::FLOAT},
      {"current", LogicalType::FLOAT},
      {"voltage", LogicalType::FLOAT}
  };

  for (auto col : cols) {
    return_names.emplace_back(std::get<0>(col));
    return_types.emplace_back(std::get<1>(col));

  }
}

static unique_ptr<FunctionData> ReadOMLBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &return_names) {
  auto result = make_uniq<OMLFunctionData>();
  result->file = StringValue::Get(input.inputs[0]);
  InitTable(return_types, return_names);
  return std::move(result);
}

unique_ptr<GlobalTableFunctionState> ReadOMLInit(ClientContext &context, TableFunctionInitInput &input) {
  return make_uniq<OMLData>();
}

static void AddRow(DataChunk &output, std::vector<string> &rowData, int &rowIndex, int priorLinesRead) {
  output.SetValue(0, rowIndex, Value(priorLinesRead + rowIndex));
  output.SetValue(1, rowIndex, Value(std::stof(rowData[3]) + std::stof(rowData[4])));
  output.SetValue(2, rowIndex, Value(std::stof(rowData[5])));
  output.SetValue(3, rowIndex, Value(std::stof(rowData[6])));
  output.SetValue(4, rowIndex, Value(std::stof(rowData[7])));
  rowIndex++;
}

static void SkipHeader(std::basic_ifstream<char> &file, string &line) {
  while (getline (file, line)) {
    if (line.empty()) return;
  }
}

static void SkipAlreadyRead(std::basic_ifstream<char> &file, string &line, int linesRead) {
  for (int i = 0; i < linesRead; i++) {
    getline(file, line);
  }
}

static LogicalType convertToLogicalType(std::string &type) {
  if (type == "uint32") return LogicalType::UINTEGER;
  if (type == "double") return LogicalType::DOUBLE;
  if (type == "string") return LogicalType::VARCHAR;
  throw ExceptionFormatValue("Unknown type: " + type);
}

static unique_ptr<FunctionData> OmlGenBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &return_names) {
  auto result = make_uniq<OMLFunctionData>();
  result->file = StringValue::Get(input.inputs[0]);
  string line;
  std::ifstream file(result->file);
  for (int i = 0; i < 7; i++) {
    getline (file, line);
  }
  auto split = StringUtil::Split(line, " ");
  for (ulong i = 3; i < split.size(); i++) {
    auto name_type = StringUtil::Split(split[i], ":");
    return_names.emplace_back(name_type[0]);
    return_types.emplace_back(convertToLogicalType(name_type[1]));
  }
  return std::move(result);
}

static void OmlGenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto linesPerRun = 1000;
  auto &bind_data = data_p.bind_data->CastNoConst<OMLFunctionData>();
  auto &data = data_p.global_state->Cast<OMLData>();
  if (data.finished) return;

  int rowCount = 0;

  string line;
  std::ifstream file(bind_data.file);
  SkipHeader(file, line);
  SkipAlreadyRead(file, line, data.linesRead);

  for (int i = 0; i < linesPerRun; i++) {
    if (!getline (file, line)) {
      data.finished = true;
      break;
    }
    auto parts = StringUtil::Split(line, "\t");
    for (ulong j = 3; j < parts.size(); j++) {
      output.SetValue(j - 3, rowCount, Value(parts[j]));
    }
    rowCount++;
  }

  data.linesRead += rowCount;
  output.SetCardinality(rowCount);
  file.close();
}

static void ReadOMLFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto linesPerRun = 1000;
  auto &bind_data = data_p.bind_data->CastNoConst<OMLFunctionData>();
  auto &data = data_p.global_state->Cast<OMLData>();
  if (data.finished) return;
//  output.Initialize(context, bind_data.return_types, 1);

  int rowCount = 0;

  string line;
  std::ifstream file(bind_data.file);
  SkipHeader(file, line);
  SkipAlreadyRead(file, line, data.linesRead);


  for (int i = 0; i < linesPerRun; i++) {
    if (!getline (file, line)) {
      data.finished = true;
      break;
    }
    auto parts = StringUtil::Split(line, "\t");
    if (parts.size() != 8) continue;
    AddRow(output, parts, rowCount, data.linesRead);
  }
  data.linesRead += rowCount;
  output.SetCardinality(rowCount);
  file.close();
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    TableFunction read_oml("read_oml", {LogicalType::VARCHAR}, ReadOMLFunction, ReadOMLBind, ReadOMLInit);
    ExtensionUtil::RegisterFunction(instance, read_oml);
    TableFunction oml_gen("oml_gen", {LogicalType::VARCHAR}, OmlGenFunction, OmlGenBind, ReadOMLInit);
    ExtensionUtil::RegisterFunction(instance, oml_gen);
}

void OmlExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string OmlExtension::Name() {
	return "oml";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void oml_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *oml_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
