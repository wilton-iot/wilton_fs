/*
 * Copyright 2017, alex at staticlibs.net
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* 
 * File:   wiltoncall_fs.cpp
 * Author: alex
 *
 * Created on May 27, 2017, 12:58 PM
 */

#include <functional>
#include <memory>
#include <vector>

#include "staticlib/io.hpp"
#include "staticlib/json.hpp"
#include "staticlib/ranges.hpp"
#include "staticlib/support.hpp"
#include "staticlib/utils.hpp"
#include "staticlib/tinydir.hpp"

#include "wilton/support/buffer.hpp"
#include "wilton/support/exception.hpp"
#include "wilton/support/logging.hpp"
#include "wilton/support/registrar.hpp"
#include "wilton/support/tl_registry.hpp"

namespace wilton {
namespace fs {

namespace { //anonymous

const std::string logger = std::string("wilton.fs");

class file_writer {
    sl::io::buffered_sink<sl::tinydir::file_sink> sink;
    bool hex;

public:
    file_writer(sl::io::buffered_sink<sl::tinydir::file_sink>&& fsink, bool use_hex) :
    sink(std::move(fsink)),
    hex(use_hex) { }
    
    file_writer(const file_writer&) = delete;
    
    file_writer& operator=(const file_writer&) = delete;
    
    file_writer(file_writer&& other) :
    sink(std::move(other.sink)),
    hex(other.hex) {
        other.hex = false;
    }

    sl::io::buffered_sink<sl::tinydir::file_sink>& get_sink() {
        return sink;
    }

    const std::string& path() {
        return sink.get_sink().path();
    }

    bool is_hex() {
        return hex;
    }
};

// initialized from wilton_module_init
std::shared_ptr<support::tl_registry<file_writer>> local_registry() {
    static auto registry = std::make_shared<support::tl_registry<file_writer>>();
    return registry;
}

} // namespace

support::buffer exists(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto rpath = std::ref(sl::utils::empty_string());
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("path" == name) {
            rpath = fi.as_string_nonempty_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (rpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'path' not specified"));
    const std::string& path = rpath.get();
    // call 
    try {
        auto tpath = sl::tinydir::path(path);
        return support::make_json_buffer({
            { "exists", tpath.exists() }
        });
    } catch (const std::exception&) {
        return support::make_json_buffer({
            { "exists", false }
        });
    }
}

support::buffer mkdir(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto rpath = std::ref(sl::utils::empty_string());
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("path" == name) {
            rpath = fi.as_string_nonempty_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (rpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'path' not specified"));
    const std::string& path = rpath.get();
    // call 
    try {
        sl::tinydir::create_directory(path);
        return support::make_null_buffer();
    } catch (const std::exception& e) {
        throw support::exception(TRACEMSG(e.what()));
    }
}

support::buffer readdir(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto rpath = std::ref(sl::utils::empty_string());
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("path" == name) {
            rpath = fi.as_string_nonempty_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (rpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'path' not specified"));
    const std::string& path = rpath.get();
    // call 
    try {
        auto li = sl::tinydir::list_directory(path);
        auto ra = sl::ranges::transform(li, [](const sl::tinydir::path & pa) -> sl::json::value {
            return sl::json::value(pa.filename());
        });
        return support::make_json_buffer(ra.to_vector());
    } catch (const std::exception& e) {
        throw support::exception(TRACEMSG(e.what()));
    }
}

support::buffer read_file(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto rpath = std::ref(sl::utils::empty_string());
    auto hex = false;
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("path" == name) {
            rpath = fi.as_string_nonempty_or_throw(name);
        } else if ("hex" == name) {
            hex = fi.as_bool_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (rpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'path' not specified"));
    const std::string& path = rpath.get();
    // call 
    try {
        auto src = sl::tinydir::file_source(path);
        if (hex) {
            auto bufsrc = sl::io::make_buffered_source(src);
            return support::make_hex_buffer(bufsrc);
        } else {
            return support::make_source_buffer(src);
        }
    } catch (const std::exception& e) {
        throw support::exception(TRACEMSG(e.what()));
    }
}

support::buffer read_lines(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto rpath = std::ref(sl::utils::empty_string());
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("path" == name) {
            rpath = fi.as_string_nonempty_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (rpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'path' not specified"));
    const std::string& path = rpath.get();
    // call 
    try {
        auto vec = std::vector<sl::json::value>();
        auto src = sl::io::make_buffered_source(sl::tinydir::file_source(path));
        for (;;) {
            auto line = src.read_line();
            if (line.empty()) break;
            if ('\r' == line.back()) {
                line.pop_back();
            }
            if (!line.empty()) { // can be empty only for "^\r\n$" lines
                vec.emplace_back(std::move(line));
            }
        }
        auto res = sl::json::value(std::move(vec));
        return support::make_json_buffer(res);
    } catch (const std::exception& e) {
        throw support::exception(TRACEMSG(e.what()));
    }
}

support::buffer realpath(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto rpath = std::ref(sl::utils::empty_string());
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("path" == name) {
            rpath = fi.as_string_nonempty_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (rpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'path' not specified"));
    const std::string& path = rpath.get();
    // call 
    try {
        auto abs = sl::tinydir::full_path(path);
        return support::make_string_buffer(abs);
    } catch (const std::exception& e) {
        throw support::exception(TRACEMSG(e.what()));
    }
}

support::buffer rename(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto roldpath = std::ref(sl::utils::empty_string());
    auto rnewpath = std::ref(sl::utils::empty_string());
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("oldPath" == name) {
            roldpath = fi.as_string_nonempty_or_throw(name);
        } else if ("newPath" == name) {
            rnewpath = fi.as_string_nonempty_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (roldpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'oldPath' not specified"));
    if (rnewpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'newPath' not specified"));    
    const std::string& oldpath = roldpath.get();
    const std::string& newpath = rnewpath.get();
    // call 
    try {
        auto old = sl::tinydir::path(oldpath);
        old.rename(newpath);
        return support::make_null_buffer();
    } catch (const std::exception& e) {
        throw support::exception(TRACEMSG(e.what()));
    }
}

support::buffer rmdir(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto rpath = std::ref(sl::utils::empty_string());
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("path" == name) {
            rpath = fi.as_string_nonempty_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (rpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'path' not specified"));
    const std::string& path = rpath.get();
    // call
    try {
        auto tpath = sl::tinydir::path(path);
        if (tpath.is_directory()) {
            tpath.remove();
        } else {
            throw support::exception(TRACEMSG("Invalid directory path: [" + path + "]"));
        }
        return support::make_null_buffer();
    } catch (const std::exception& e) {
        throw support::exception(TRACEMSG(e.what()));
    }
}

support::buffer stat(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto rpath = std::ref(sl::utils::empty_string());
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("path" == name) {
            rpath = fi.as_string_nonempty_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (rpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'path' not specified"));
    const std::string& path = rpath.get();
    // call
    try {
        auto tpath = sl::tinydir::path(path);
        return support::make_json_buffer({
            { "size", tpath.is_regular_file() ? static_cast<int64_t>(tpath.open_read().size()) : 0 },
            { "isFile", tpath.is_regular_file() },
            { "isDirectory", tpath.is_directory() }
        });
    } catch (const std::exception& e) {
        throw support::exception(TRACEMSG(e.what()));
    }
}

support::buffer unlink(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto rpath = std::ref(sl::utils::empty_string());
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("path" == name) {
            rpath = fi.as_string_nonempty_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (rpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'path' not specified"));
    const std::string& path = rpath.get();
    // call
    try {
        auto tpath = sl::tinydir::path(path);
        if (tpath.is_regular_file()) {
            tpath.remove();
        } else {
            throw support::exception(TRACEMSG("Invalid file path: [" + path + "]"));
        }
        return support::make_null_buffer();
    } catch (const std::exception& e) {
        throw support::exception(TRACEMSG(e.what()));
    }
}

support::buffer copy_file(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto roldpath = std::ref(sl::utils::empty_string());
    auto rnewpath = std::ref(sl::utils::empty_string());
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("oldPath" == name) {
            roldpath = fi.as_string_nonempty_or_throw(name);
        } else if ("newPath" == name) {
            rnewpath = fi.as_string_nonempty_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (roldpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'oldPath' not specified"));
    if (rnewpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'newPath' not specified"));
    const std::string& oldpath = roldpath.get();
    const std::string& newpath = rnewpath.get();
    // call 
    try {
        auto old = sl::tinydir::path(oldpath);
        old.copy_file(newpath);
        return support::make_null_buffer();
    } catch (const std::exception& e) {
        throw support::exception(TRACEMSG(e.what()));
    }
}

support::buffer open_tl_file_writer(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto rpath = std::ref(sl::utils::empty_string());
    auto hex = false;
    auto append = false;
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("path" == name) {
            rpath = fi.as_string_nonempty_or_throw(name);
        } else if ("hex" == name) {
            hex = fi.as_bool_or_throw(name);
        } else if ("append" == name) {
            append = fi.as_bool_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (rpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'path' not specified"));
    const std::string& path = rpath.get();
    // create writer
    try {
        auto reg = local_registry();
        auto mode = append ? sl::tinydir::file_sink::open_mode::append :
                sl::tinydir::file_sink::open_mode::create;
        auto fsink = sl::tinydir::file_sink(path, mode);
        auto sink = sl::io::make_buffered_sink(std::move(fsink));
        auto writer = file_writer(std::move(sink), hex);
        reg->put(std::move(writer));
        wilton::support::log_debug(logger, std::string("TL file writer opened,") + 
                " path: [" + path + "], append: [" + (append ? "true" : "false") + "]");
        return support::make_null_buffer();
    } catch (const std::exception& e) {
        throw support::exception(TRACEMSG(e.what()));
    }
}

support::buffer append_tl_file_writer(sl::io::span<const char> data) {
    auto reg = local_registry();
    auto& writer = reg->peek();
    auto src = sl::io::array_source(data.data(), data.size());
    auto& sink = writer.get_sink();
    size_t written = 0;
    if (writer.is_hex()) {
        auto unhexer = sl::io::make_hex_source(src);
        written = sl::io::copy_all(unhexer, sink);
    } else {
        written = sl::io::copy_all(src, sink);
    }
    wilton::support::log_debug(logger, std::string("TL file writer appended,") + 
            " path: [" + writer.path() + "]," +
            " bytes: [" + sl::support::to_string(written) + "]");
    return support::make_null_buffer();
}

support::buffer close_tl_file_writer(sl::io::span<const char>) {
    auto reg = local_registry();
    {
        // will be destroyed at the end of scope
        // no reinsertion logic on unlikely error
        auto writer = reg->remove();
        wilton::support::log_debug(logger, std::string("TL file writer closed,") +
                " path: [" + writer.path() + "]");
    }
    return support::make_null_buffer();
}

support::buffer symlink(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto rdest = std::ref(sl::utils::empty_string());
    auto rlink = std::ref(sl::utils::empty_string());
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("dest" == name) {
            rdest = fi.as_string_nonempty_or_throw(name);
        } else if ("link" == name) {
            rlink = fi.as_string_nonempty_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (rdest.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'dest' not specified"));
    if (rlink.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'link' not specified"));    
    const std::string& dest = rdest.get();
    const std::string& link = rlink.get();
    // call 
    try {
        sl::tinydir::create_symlink(dest, link);
        return support::make_null_buffer();
    } catch (const std::exception& e) {
        throw support::exception(TRACEMSG(e.what()));
    }
}

support::buffer insert_file(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto rsource_path = std::ref(sl::utils::empty_string());
    auto rdest_path = std::ref(sl::utils::empty_string());
    int32_t offset = 0;
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("sourcePath" == name) {
            rsource_path = fi.as_string_nonempty_or_throw(name);
        } else if ("destPath" == name) {
            rdest_path = fi.as_string_nonempty_or_throw(name);
        } else if ("offset" == name) {
            offset = fi.as_int32_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (rsource_path.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'sourcePath' not specified"));
    if (rdest_path.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'destPath' not specified"));

    const std::string& source_path = rsource_path.get();
    const std::string& dest_path = rdest_path.get();

    // call
    try {
        auto dp = sl::tinydir::path(dest_path);
        auto dest = dp.open_write(sl::tinydir::file_sink::open_mode::from_file);
        dest.seek(offset);
        dest.write_from_file(source_path);
        return support::make_null_buffer();
    } catch (const std::exception& e) {
        throw support::exception(TRACEMSG(e.what()));
    }
}

support::buffer resize_file(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto rpath = std::ref(sl::utils::empty_string());
    size_t new_size = 0;
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("path" == name) {
            rpath = fi.as_string_nonempty_or_throw(name);
        } else if ("size" == name) {
            new_size = fi.as_int32_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (rpath.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'path' not specified"));
    const std::string& path = rpath.get();
    // call
    try {
        auto tpath = sl::tinydir::path(path);
        tpath.resize(new_size);

        return support::make_null_buffer();
    } catch (const std::exception& e) {
        throw support::exception(TRACEMSG(e.what()));
    }
}


} // namespace
}

extern "C" char* wilton_module_init() {
    try {
        wilton::fs::local_registry();

        wilton::support::register_wiltoncall("fs_exists", wilton::fs::exists);
        wilton::support::register_wiltoncall("fs_mkdir", wilton::fs::mkdir);
        wilton::support::register_wiltoncall("fs_readdir", wilton::fs::readdir);
        wilton::support::register_wiltoncall("fs_read_file", wilton::fs::read_file);
        wilton::support::register_wiltoncall("fs_read_lines", wilton::fs::read_lines);
        wilton::support::register_wiltoncall("fs_realpath", wilton::fs::realpath);
        wilton::support::register_wiltoncall("fs_rename", wilton::fs::rename);
        wilton::support::register_wiltoncall("fs_rmdir", wilton::fs::rmdir);
        wilton::support::register_wiltoncall("fs_stat", wilton::fs::stat);
        wilton::support::register_wiltoncall("fs_unlink", wilton::fs::unlink);
        wilton::support::register_wiltoncall("fs_copy_file", wilton::fs::copy_file);
        wilton::support::register_wiltoncall("fs_open_tl_file_writer", wilton::fs::open_tl_file_writer);
        wilton::support::register_wiltoncall("fs_append_tl_file_writer", wilton::fs::append_tl_file_writer);
        wilton::support::register_wiltoncall("fs_close_tl_file_writer", wilton::fs::close_tl_file_writer);
        wilton::support::register_wiltoncall("fs_symlink", wilton::fs::symlink);
        wilton::support::register_wiltoncall("fs_insert_file", wilton::fs::insert_file);
        wilton::support::register_wiltoncall("fs_resize_file", wilton::fs::resize_file);
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
}
