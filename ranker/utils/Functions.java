package utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Functions {
	public static int graphLastVersion(final String path, final String prefix) {
		File dir = new File(path);

		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File dir, String fname) {
				return fname.startsWith(prefix + "__");
			}
		};

		String[] children = dir.list(filter);
		if ( children == null ) return 0;

		Integer val = null;
		for(int ch = 0; ch < children.length; ++ch) {
			String[] parts = children[ch].split("-");
			if (parts.length < 2) continue;
			if ( val == null || Integer.valueOf(parts[1]).compareTo(val) > 0 )
				val = Integer.valueOf(parts[1]);
		}

		if ( val == null ) return 0;
		else return val;
	}
	
	public static Object loadObject(String path, String name) throws Throwable {
		Object robject = null;
		try {
			FileInputStream file = new FileInputStream(path + "/" + name);
			ObjectInputStream obj = new ObjectInputStream(file);
			robject = obj.readObject();
		} catch (FileNotFoundException e) {
			// Ignore exception, the returned object will be null
		} catch (Throwable t) { 
			new Exception("Error loading the persistent graph (file: " +name+ ")", t);
		}
		return robject;
	}

	public static void saveObject(String path, String name, Object obj)  throws Throwable {
		try {
			FileOutputStream fObject   = new FileOutputStream(path + "/" + name);
			ObjectOutputStream oObject = new ObjectOutputStream(fObject);
			oObject.writeObject(obj);
			oObject.close();
		} catch ( Throwable t ) {
			new Exception("Error saving the persistent graph (file: " +name+ ")", t);
		}
	}
	
	public static <V> HashSet<V> SetIntersection(Set<V> a, Set<V> b) {
		HashSet<V> r = new HashSet<V>();
		for( V va : a )	{ if ( b.contains(va) ) r.add(va); }
		return r;
	}
	
	public static <K,V> HashMap<K, ArrayList<V>> MapOfSetsToMapOfLists (Map<K, HashSet<V>> in) {
		HashMap<K,ArrayList<V>> out = new HashMap<K,ArrayList<V>>();
		for ( Map.Entry<K, HashSet<V>> entry : in.entrySet() ) {
			out.put(entry.getKey(), new ArrayList<V>(entry.getValue()));
		}
		return out;
	}	
}
