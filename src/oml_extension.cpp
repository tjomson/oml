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

namespace duckdb {

struct OMLFunctionData : public TableFunctionData {
  string file;
  vector<LogicalType> return_types;
};

struct OMLData : public GlobalTableFunctionState {
  bool finished = false;
};

static void InitTable(vector<LogicalType> &return_types, vector<string> &return_names) {
  return_names.emplace_back("experiment_id");
  return_types.emplace_back(LogicalType::VARCHAR);
  return_names.emplace_back("node_id");
  return_types.emplace_back(LogicalType::VARCHAR);
  return_names.emplace_back("node_id_seq");
  return_types.emplace_back(LogicalType::VARCHAR);
  return_names.emplace_back("time_sec");
  return_types.emplace_back(LogicalType::VARCHAR);
  return_names.emplace_back("time_usec");
  return_types.emplace_back(LogicalType::VARCHAR);
  return_names.emplace_back("power");
  return_types.emplace_back(LogicalType::FLOAT);
  return_names.emplace_back("current");
  return_types.emplace_back(LogicalType::FLOAT);
  return_names.emplace_back("voltage");
  return_types.emplace_back(LogicalType::FLOAT);
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

static void ReadOMLFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  std::cout << "start oml function" << std::endl;
  auto &bind_data = data_p.bind_data->CastNoConst<OMLFunctionData>();
  auto &data = data_p.global_state->Cast<OMLData>();
  if (data.finished) return;
//  output.Initialize(context, bind_data.return_types, 1);
//  for (auto type :output.GetTypes()) {
//    std::cout << type.ToString() << std::endl;
//  }
  for (int i = 0; i < 3; i++) {
    output.SetValue(0, i, Value("hej0"+std::to_string(i)));
    output.SetValue(1, i, Value("hej1"+std::to_string(i)));
    output.SetValue(2, i, Value("hej2"+std::to_string(i)));
    output.SetValue(3, i, Value("hej3"+std::to_string(i)));
    output.SetValue(4, i, Value("hej4"+std::to_string(i)));
    output.SetValue(5, i, Value(0.1 + i));
    output.SetValue(6, i, Value(0.2 + i));
    output.SetValue(7, i, Value(0.3 + i));
  }
  output.SetCardinality(3);
  data.finished = true;

//  string line;
//  std::ifstream file(bind_data.file);
//  while (getline (file, line)) {
////    std::cout << line << std::endl;
//  }
//  file.close();
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
