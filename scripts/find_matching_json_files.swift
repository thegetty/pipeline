/**************

 ./find_matching_json_files /path/to/post_sale_rewrite_map.json PATH
  
 Find and print the path to JSON files in PATH that contain at least one string value
 that appears as a key in the post_sale_rewrite_map.json file.
 
 **************/

import Foundation

let args = CommandLine.arguments
if args.count != 3 {
	print("Usage: \(args[0]) post_sale_rewrite_map.json PATH")
	exit(1)
}
let map_file = args[1]
let path = args[2]
let pathu = URL(fileURLWithPath: path)

let PARALLEL = true
let u = URL(fileURLWithPath: map_file)
let d = try Data(contentsOf: u)
let map = try JSONSerialization.jsonObject(with: d) as! [String:String]
let map_keys = Set(map.keys)

let output_queue = DispatchQueue(label: "edu.getty.digital.2019.pipeline.post-sales-output")

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

func _find(_ value: Any, _ map: [String:String]) -> String? {
    if let d = value as? [String: Any] {
        for v in d.values {
            if let m = _find(v, map) {
                return m
            }
        }
    } else if let a = value as? [Any] {
        for v in a {
            if let m = _find(v, map) {
                return m
            }
        }
    } else if let s = value as? String {
        if map_keys.contains(s) {
            return s
        }
    }
    return nil
}

var count = 0
let start = DispatchTime.now()
let process_queue = DispatchQueue(label: "edu.getty.digital.2019.pipeline.post-sales-rewriting", attributes: .concurrent)
walk(path: pathu, queue: process_queue) { (file) in
    count += 1
//   print("\r\(count)             ", separator: "", terminator: "", to: &stderr)
    do {
        let d = try Data(contentsOf: file)
        let j = try JSONSerialization.jsonObject(with: d)
        if let m = _find(j, map) {
//            print("\r\(file)\t--> \(m)")
//            print("\(m)\t\(file)")
			if PARALLEL {
				output_queue.async {
					print("\(file.path)")
				}
			} else {
				print("\(file.path)")
			}
        }
    } catch {}
}
let end = DispatchTime.now()
let nanoTime = end.uptimeNanoseconds - start.uptimeNanoseconds
let elapsed = Double(nanoTime) / 1_000_000_000
//print("\(count) file processed in \(elapsed)s")
