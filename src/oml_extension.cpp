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
#include <iostream>
#include <openssl/opensslv.h>

namespace duckdb {

struct OMLData : public TableFunctionData {
  vector<string> files;
  idx_t filename_col_idx;
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

static unique_ptr<FunctionData> ReadOMLBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &return_names) {
  auto result = make_uniq<OMLData>();
  result->files = MultiFileReader::GetFileList(context, input.inputs[0], "OML");
  return_types.emplace_back(LogicalType::VARCHAR);
  return_names.emplace_back("stuff");
//  result->return_types.emplace_back(LogicalType::VARCHAR);
//  result->return_names.emplace_back("stuff");
  std::cout << "bind done" << std::endl;
  return std::move(result);
}

static void ReadOMLFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  std::cout << "start oml function" << std::endl;
  auto &bind_data = data_p.bind_data->CastNoConst<OMLData>();
//  auto &data = data_p.global_state->Cast<GlobalTableFunctionState>();
//  auto &lstate = data_p.local_state->Cast<LocalTableFunctionState>();
  std::cout << "lmao " << bind_data.files[0] << std::endl;

//  std::cout << "lmao" << std::endl;
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
    TableFunction read_oml("read_oml", {LogicalType::VARCHAR}, ReadOMLFunction, ReadOMLBind);
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
