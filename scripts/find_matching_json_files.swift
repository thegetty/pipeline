import Foundation

let args = CommandLine.arguments
let map_file = args[1]
let path = args[2]
let pathu = URL(fileURLWithPath: path)

let u = URL(fileURLWithPath: map_file)
let d = try Data(contentsOf: u)
let map = try JSONSerialization.jsonObject(with: d) as! [String:String]
let map_keys = Set(map.keys)

let filemgr = FileManager.default
let resourceKeys : [URLResourceKey] = [.nameKey, .isDirectoryKey]
let directoryEnumerator = filemgr.enumerator(
    at: pathu,
    includingPropertiesForKeys: resourceKeys,
    options: [.skipsHiddenFiles],
    errorHandler: nil
)

func walk(path pathu: URL, _ callback : (URL) -> ()) {
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
                callback(fileURL as URL)
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
walk(path: pathu) { (file) in
    count += 1
//   print("\r\(count)             ", separator: "", terminator: "", to: &stderr)
    do {
        let d = try Data(contentsOf: file)
        let j = try JSONSerialization.jsonObject(with: d)
        if let m = _find(j, map) {
//            print("\r\(file)\t--> \(m)")
//            print("\(m)\t\(file)")
            print("\(file.path)")
        }
    } catch {}
}
