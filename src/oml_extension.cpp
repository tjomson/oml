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

//struct OMLLocalstate : public LocalTableFunctionState {
//public:
//  explicit OMLLocalstate() : bytes_read(0), total_size(0), current_progress(0), file_index(0) {
//  }
//
//  //! The CSV reader
//  unique_ptr<BufferedCSVReader> csv_reader;
//  //! The current amount of bytes read by this reader
//  idx_t bytes_read;
//  //! The total amount of bytes in the file
//  idx_t total_size;
//  //! The current progress from 0..100
//  idx_t current_progress;
//  //! The file index of this reader
//  idx_t file_index;
//};

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
//  auto &data = data_p.global_state->Cast<GlobalTableFunctionState>();
//  auto &lstate = data_p.local_state->Cast<LocalTableFunctionState>();
//  std::cout << "lmao " << bind_data.file << std::endl;
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

//  std::cout << output.ToString() << std::endl;


//  string line;
//  std::ifstream file(bind_data.file);
//  while (getline (file, line)) {
////    std::cout << line << std::endl;
//  }
//  file.close();
}

//inline void OmlScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
//  auto &name_vector = args.data[0];
//    UnaryExecutor::Execute<string_t, string_t>(
//	    name_vector, result, args.size(),
//	    [&](string_t name) {
//			return StringVector::AddString(result, "Omqweqel12 "+name.GetString()+" 🐥hahweha");
//        });
//}
//
//inline void OmlOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
//    auto &name_vector = args.data[0];
//    UnaryExecutor::Execute<string_t, string_t>(
//	    name_vector, result, args.size(),
//	    [&](string_t name) {
//			return StringVector::AddString(result, "Oml " + name.GetString() +
//                                                     ", my linked OpenSSL version is " +
//                                                     OPENSSL_VERSION_TEXT );;
//        });
//}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
//    auto oml_scalar_function = ScalarFunction("oml", {LogicalType::VARCHAR}, LogicalType::VARCHAR, OmlScalarFun);
//    ExtensionUtil::RegisterFunction(instance, oml_scalar_function);
    TableFunction read_oml("read_oml", {LogicalType::VARCHAR}, ReadOMLFunction, ReadOMLBind, ReadOMLInit);
    ExtensionUtil::RegisterFunction(instance, read_oml);

    // Register another scalar function
//    auto oml_openssl_version_scalar_function = ScalarFunction("oml_openssl_version", {LogicalType::VARCHAR},
//                                                LogicalType::VARCHAR, OmlOpenSSLVersionScalarFun);
//    ExtensionUtil::RegisterFunction(instance, oml_openssl_version_scalar_function);
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
