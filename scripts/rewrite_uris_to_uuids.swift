/**************

 ./generate_uri_uuids /path/to/uri_to_uuid-map.json PATH PREFIX

 Read through all JSON files in PATH, finding all URIs that have PREFIX as a prefix.
 For each such URI (and after removing the common prefix), if it does not appear as
 a key in the uri_to_uuid-map.json file, generate a new urn:uuid: URI, and add it as
 an entry to uri_to_uuid-map.json (which is updated in place).
 
 **************/

import Foundation

let args = CommandLine.arguments
if args.count != 4 {
	print("Usage: \(args[0]) PREFIX uri_to_uuid-map.json PATH")
	exit(1)
}
let prefix = args[1]
let map_file = args[2]
let path = args[3]
let pathu = URL(fileURLWithPath: path)

let PARALLEL = true
let map_file_url = URL(fileURLWithPath: map_file)
let filemgr = FileManager.default
let uri_to_uuid_map : [String:String]
if filemgr.fileExists(atPath: map_file) {
	let d = try Data(contentsOf: map_file_url)
	let decoder = JSONDecoder()
	uri_to_uuid_map = try decoder.decode([String:String].self, from: d)
} else {
	uri_to_uuid_map = [:]
}

func walk(path pathu: URL, queue: DispatchQueue, _ callback : @escaping (URL) -> ()) {
	let filemgr = FileManager.default
	let resourceKeys : [URLResourceKey] = [.nameKey, .isDirectoryKey]
	let directoryEnumerator = filemgr.enumerator(
		at: pathu,
		includingPropertiesForKeys: resourceKeys,
		options: [.skipsHiddenFiles],
		errorHandler: nil
	)
    if let directoryEnumerator = directoryEnumerator {
        for case let fileURL as NSURL in directoryEnumerator {
            guard let resourceValues = try? fileURL.resourceValues(forKeys: resourceKeys),
                let isDirectory = resourceValues[.isDirectoryKey] as? Bool,
                let name = resourceValues[.nameKey] as? String
                else {
                    continue
            }
            
            if isDirectory {
            	if name == "tmp" {
	            	directoryEnumerator.skipDescendants()
	            }
            } else {
//                print("Processing: \(fileURL)")
            	if PARALLEL {
					queue.async {
						callback(fileURL as URL)
					}
            	} else {
					callback(fileURL as URL)
            	}
            }
        }
    }
}

func filename_for_data(_ value: Any) -> String? {
    guard let value = value as? [String:Any] else {
        print("*** failed to determine filename: data is not a dictionary")
        return nil
    }
    guard let id = value["id"] else {
        print("*** failed to determine filename: no id")
        return nil
    }
    guard let id_string = id as? String else {
        print("*** failed to determine filename: id is not a string: \(id)")
        return nil
    }
    guard id_string.hasPrefix("urn:uuid:") else {
        print("*** failed to determine filename: id is not a urn:uuid: URL: \(id_string)")
        return nil
    }
    let i = id_string.index(id_string.startIndex, offsetBy: 9)
    let s = id_string[i...]
    let filename = "\(s).json"
    return filename
}

func rewrite_uris(_ value: Any, _ prefix: String, uri_to_uuid_map: [String:String]) -> Any {
    if let d = value as? [String: Any] {
        var rewritten = [String: Any]()
        for (k, v) in d {
            rewritten[k] = rewrite_uris(v, prefix, uri_to_uuid_map: uri_to_uuid_map)
        }
        return rewritten
    } else if let a = value as? [Any] {
        var rewritten = [Any]()
        for v in a {
        	let m = rewrite_uris(v, prefix, uri_to_uuid_map: uri_to_uuid_map)
            rewritten.append(m)
        }
        return rewritten
    } else if let s = value as? String {
    	if s.hasPrefix(prefix) {
            let i = s.index(s.startIndex, offsetBy: prefix.count)
            let suffix = String(s[i...])
            guard let value = uri_to_uuid_map[suffix] else {
                fatalError("Failed to find UUID mapping for: \(suffix)")
            }
//            print("found key with prefix [\(suffix)] --> \(value)")
            return value
        } else {
            return s
        }
    }
    return value
}

func write_file(_ filename: String, _ url: URL, _ data: Data, _ queue: DispatchQueue) throws {
    var new_url = url.deletingLastPathComponent().appendingPathComponent(filename)
    if url == new_url {
        if PARALLEL {
            queue.async {
                do {
                    try data.write(to: new_url, options: [.atomic])
                } catch let e {
                    print("*** failed to write file \(new_url): \(e)")
                }
            }
        } else {
            do {
                try data.write(to: new_url, options: [.atomic])
            } catch let e {
                print("*** failed to write file \(new_url): \(e)")
            }
        }
    } else {
        let orig_url = new_url
        queue.async { () -> Void in
            var i = 0
            while true {
                if FileManager.default.fileExists(atPath: new_url.path) {
                    if i == 0 {
                        i = 1
                        new_url = new_url.appendingPathExtension("\(i)")
                    } else {
                        i += 1
                        new_url = new_url.deletingPathExtension().appendingPathExtension("\(i)")
                    }
                    print("*** Will not overwrite file at \(orig_url); using \(new_url)")
                    continue
                }
                break
            }
            //            print("*** filename changed from \(url.lastPathComponent) to \(new_url.lastPathComponent)")
            do {
                try data.write(to: new_url, options: [.atomic])
                try? FileManager.default.removeItem(at: url)
            } catch let e {
                print("*** failed to replace \(url) with \(new_url): \(e)")
            }
        }
    }
}

func process(uri_to_uuid_map: [String:String], path pathu: URL, prefix: String) {
	var count = 0
	let start = DispatchTime.now()
	let serial = DispatchQueue(label: "edu.getty.digital.2019.pipeline.uri-to-uuid-sync")
	let process_queue = DispatchQueue(label: "edu.getty.digital.2019.pipeline.uri-to-uuid-collection", attributes: .concurrent)
	walk(path: pathu, queue: process_queue) { (file) in
        guard file.pathExtension == "json" else {
            return
        }
        
		count += 1
		if count % 1000 == 0 {
			print("\r\(count) files processed    ", terminator: "")
		}
	//	print("\r\(count)             ", separator: "", terminator: "")
		do {
			let d = try Data(contentsOf: file)
            guard let j = try? JSONSerialization.jsonObject(with: d) else {
                print("*** Failed to parse JSON from \(file)")
                return
            }
            let rewritten = rewrite_uris(j, prefix, uri_to_uuid_map: uri_to_uuid_map)
            let data = try JSONSerialization.data(withJSONObject: rewritten)
            guard let filename = filename_for_data(rewritten) else {
                print("*** could not find filename in JSON from \(file)")
                if true {
                    let s = String(data: data, encoding: .utf8)!
                    print(s)
                }
                return
            }
			if PARALLEL {
				serial.async {
                    do {
                        try write_file(filename, file, data, serial)
                    } catch let e {
                        print("*** error caught while writing \(file): \(e)")
                    }
				}
			} else {
                do {
                    try write_file(filename, file, data, serial)
                } catch let e {
                    print("*** error caught while writing \(file): \(e)")
                }
			}
		} catch let e {
            print("*** failed walk on \(file): \(e)")
        }
	}
	print("\n")
	process_queue.sync(flags: .barrier) {}
    serial.sync(flags: .barrier) {}

	let end = DispatchTime.now()
	let nanoTime = end.uptimeNanoseconds - start.uptimeNanoseconds
	let elapsed = Double(nanoTime) / 1_000_000_000
	
    let fps = String(format: "%.1f files/second", Double(count) / elapsed)
	print("\(count) file processed in \(elapsed)s (\(fps))")
}

//try! printDict("uri_to_uuid_map", uri_to_uuid_map)
process(uri_to_uuid_map: uri_to_uuid_map, path: pathu, prefix: prefix)
