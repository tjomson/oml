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
  int tuples_read = 0;
};

std::vector<std::string> splitString(const std::string& input, const std::string& delimiterRegex) {
  std::vector<std::string> result;
  std::regex regex(delimiterRegex);
  std::sregex_token_iterator iter(input.begin(), input.end(), regex, -1);
  std::sregex_token_iterator end;

  for (; iter != end; ++iter) {
    result.push_back(iter->str());
  }

  return result;
}

static void InitTable(vector<LogicalType> &return_types, vector<string> &return_names) {
  std::vector<std::tuple<std::string, LogicalType>> cols = {
      {"experiment_id", LogicalType::VARCHAR},
      {"node_id", LogicalType::VARCHAR},
      {"node_id_seq", LogicalType::VARCHAR},
      {"time-sec", LogicalType::VARCHAR},
      {"time_usec", LogicalType::VARCHAR},
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
  std::cout << "bind done " << input.inputs[0] << std::endl;
  return std::move(result);
}

unique_ptr<GlobalTableFunctionState> ReadOMLInit(ClientContext &context, TableFunctionInitInput &input) {
  return make_uniq<OMLData>();
}

static void AddRow(DataChunk &output, std::vector<string> &rowData, int index) {
  for (int i = 0; i < 5; i++) {
    output.SetValue(i, index, Value(rowData[i]));
  }
  for (int i = 5; i < 8; i++) {
    output.SetValue(i, index, Value(std::stof(rowData[i])));
  }
}

static void ReadOMLFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  std::cout << "start oml function" << std::endl;
  auto &bind_data = data_p.bind_data->CastNoConst<OMLFunctionData>();
  auto &data = data_p.global_state->Cast<OMLData>();
//  if (data.finished) return;
//  output.Initialize(context, bind_data.return_types, 1);

  string line;
  std::ifstream file(bind_data.file);
  bool dataStarted = false;
  int index = 0;
  int startIndex = data.tuples_read;
  while (getline (file, line)) {
    if (line == "") {
      dataStarted = true;
      continue;
    }
    if (!dataStarted) continue;
    if (index < startIndex) {
      index++;
      continue;
    }
    if (index > startIndex + 1000) return;
    auto parts = splitString(line, "\\s+");
    if (parts.size() != 8) continue;

    for (auto part : parts) std::cout << part << "-";
    AddRow(output, parts, data.tuples_read);
    data.tuples_read++;
    std::cout << std::endl;
  }
  output.SetCardinality(data.tuples_read);
  std::cout << output.ToString() << std::endl;
  std::cout << "row count: " << data.tuples_read << std::endl;
  file.close();
  data.finished = true;
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    TableFunction read_oml("read_oml", {LogicalType::VARCHAR}, ReadOMLFunction, ReadOMLBind, ReadOMLInit);
    ExtensionUtil::RegisterFunction(instance, read_oml);
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
