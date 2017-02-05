package tool;

import java.io.*;
import java.util.HashMap;
import java.util.Properties;

/**
 * @author James
 *
 */
public class PropertiesUtil {
	/**
	 * 
	 */
	private static HashMap<String, Properties> properyMap = new HashMap<String, Properties>(4);

	/**
	 * @param key
	 * @return
	 */
	public static Properties getProperites(String key) {
		Properties properties = null;
		if (properyMap.containsKey(key)) {
			properties = properyMap.get(key);
		} else {
			properties = new Properties();
//			InputStream stream = Properties.class.getResourceAsStream("/" + key);
            InputStream stream=PropertiesUtil.class.getResourceAsStream("/"+key);
			System.out.println();
			try {
				properties.load(stream);
			} catch (IOException e) {
				properties = null;
				e.printStackTrace();
			}
			if (properties != null) {
				properyMap.put(key, properties);
			}

		}
		return properties;
	}

    public static Properties GetProperties(String Path) throws FileNotFoundException {
        Properties properties = null;
        properties = new Properties();
        InputStream stream = new FileInputStream(new File(Path));
        try {
            properties.load(stream);
        } catch (IOException e) {
            properties = null;
            e.printStackTrace();
        }
        return properties;
    }
}
