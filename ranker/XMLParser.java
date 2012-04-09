import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.mapper.MapperWrapper;

/**
 * Parses an XML file into Java objects.
 */
public class XMLParser {
	public static void main(String[] args) {		
		if (args.length < 1) {
			System.err.println("Missing required argument: path to xml file");
		} else {
			new XMLParser(new File(args[0]));			
		}
	}
	
	public XMLParser(File data) {
		XStream xstream = createXStream();
		initiateXStream(xstream);
		Statuses test = (Statuses) xstream.fromXML(data);
		String xml = xstream.toXML(test);
		System.out.println(xml);
	}

	private void initiateXStream(XStream xstream) {
		xstream.alias("statuses", Statuses.class);
		xstream.alias("status", Status.class);
		xstream.addImplicitCollection(Statuses.class, "statuses");
	}
	
	private XStream createXStream() {
		return new XStream() {
			@Override
			protected MapperWrapper wrapMapper(MapperWrapper next) {
				return new MapperWrapper(next) {
					@SuppressWarnings("unchecked")
					@Override
					public boolean shouldSerializeMember(Class definedIn,
							String fieldName) {
						if (fieldName.equals("status")) {
							return true;
						} else if (definedIn == Object.class) {
							return false;
						}
						return super.shouldSerializeMember(definedIn, fieldName);
					}
				};
			}
		};
	}
}

class Statuses {
	protected List<Status> statuses = new ArrayList<Status>();
	public Statuses() {}
	public void add(Status status) {
		statuses.add(status);
	}
}

// TODO: expand
class Status {
	public String text;
	public long id;
	public Status(String text, long id) {
		this.text = text;
		this.id = id;
	}
}