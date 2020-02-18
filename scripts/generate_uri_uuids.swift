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
	print("Usage: \(args[0]) uri_to_uuid-map.json PATH PREFIX")
	exit(1)
}
let map_file = args[1]
let path = args[2]
let prefix = args[3]
let pathu = URL(fileURLWithPath: path)

let PARALLEL = true
let map_file_url = URL(fileURLWithPath: map_file)
let filemgr = FileManager.default
let existing_map : [String:String]
if filemgr.fileExists(atPath: map_file) {
	let d = try Data(contentsOf: map_file_url)
	let decoder = JSONDecoder()
	existing_map = try decoder.decode([String:String].self, from: d)
} else {
	existing_map = [:]
}

func assign_uuids(for uris: Set<String>, prefix: String) -> [String:String] {
	var m = [String:String]()
	for s in uris {
		let u = UUID().uuidString
		m[s] = "urn:uuid:\(u.lowercased())"
	}
	return m
}

func printDict<V: Encodable>(_ name: String, _ value: [String:V]) throws {
	let encoder = JSONEncoder()
	encoder.outputFormatting = .prettyPrinted
	let data = try encoder.encode(value)
	let string = String(data: data, encoding: .utf8)!
	print("\(name) = \(string)")
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

func find_uris(_ value: Any, _ prefix: String, exclude: Set<String>) -> Set<String> {
	var set = Set<String>()
    if let d = value as? [String: Any] {
        for v in d.values {
        	let m = find_uris(v, prefix, exclude: exclude)
        	set.formUnion(m)
        }
    } else if let a = value as? [Any] {
        for v in a {
        	let m = find_uris(v, prefix, exclude: exclude)
        	set.formUnion(m)
        }
    } else if let s = value as? String {
    	if s.hasPrefix(prefix) {
    		var k = s
			let start = k.startIndex
			let end = k.index(start, offsetBy: prefix.count)
			k.removeSubrange(start..<end)
			if !exclude.contains(k) {
	    		set.insert(k)
	    	}
        }
    }
    return set
}

func process(map: [String:String], path pathu: URL, prefix: String) -> [String:String] {
	var count = 0
	var found_uris = Set<String>()
	let start = DispatchTime.now()
	let serial = DispatchQueue(label: "edu.getty.digital.2019.pipeline.uri-to-uuid-sync")
	let process_queue = DispatchQueue(label: "edu.getty.digital.2019.pipeline.uri-to-uuid-collection", attributes: .concurrent)
	let map_keys = Set(map.keys)
	walk(path: pathu, queue: process_queue) { (file) in
		count += 1
		if count % 100 == 0 {
			print("\r\(count) files    ", terminator: "")
		}
	//	print("\r\(count)             ", separator: "", terminator: "")
		do {
			let d = try Data(contentsOf: file)
			let j = try JSONSerialization.jsonObject(with: d)
			let s = find_uris(j, prefix, exclude: map_keys)
			if PARALLEL {
				serial.async {
					found_uris.formUnion(s)
				}
			} else {
				found_uris.formUnion(s)
			}
		} catch {}
	}
	print("\n")
	process_queue.sync(flags: .barrier) {}

	let uris = found_uris.subtracting(map_keys)
	let end = DispatchTime.now()
	let nanoTime = end.uptimeNanoseconds - start.uptimeNanoseconds
	let elapsed = Double(nanoTime) / 1_000_000_000
	
	let assignments = assign_uuids(for: uris, prefix: prefix)
//	try! printDict("assignments", assignments)
	print("\(count) file processed in \(elapsed)s")
	print("\(found_uris.count) found URIs")
	print("\(map_keys.count) pre-defined URIs")
	print("\(uris.count) URIs needing assignment")

	return assignments
}

//try! printDict("existing_map", existing_map)
let new_map = process(map: existing_map, path: pathu, prefix: prefix)
let updated_map	= try existing_map.merging(new_map) { (a: String, b: String) throws -> String in
	fatalError("*** Collission: \(a), \(b)")
}
print("updated map has \(updated_map.count) elements")

let encoder = JSONEncoder()
//let map_file_url_out = URL(fileURLWithPath: "\(map_file).out")
let map_file_url_out = map_file_url
encoder.outputFormatting = [.prettyPrinted] // .sortedKeys
let data = try encoder.encode(updated_map)
try! data.write(to: map_file_url_out, options: [.atomic])
