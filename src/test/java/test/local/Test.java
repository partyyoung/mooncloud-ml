package test.local;

import java.util.HashMap;

import net.mooncloud.util.Yaml;

import org.apache.hadoop.util.StringUtils;

public class Test {
	public static void main(String[] args) {
		String line = "{'100004': '100004', '100003': '100003', '100002': '100002', '100001': '100001', '100008': '100008',\n  '100007': '100007', '100006': '100006', '100005': '100005', '100009': '100009',\n  '100000': '100000'}\n";
//		System.out.println(line);
		HashMap<String, String> map = new HashMap<String, String>();
		for(int i = 100000; i < 100010; i++)
		{
			map.put(i+"", i+"");
		}
		System.out.println(map);
		String yamlStr = new Yaml().dumpFlat(map);
//		String yamlStr = new Yaml().dumpFlat(map);
		yamlStr = StringUtils.escapeString(yamlStr, StringUtils.ESCAPE_CHAR, '\n');//yamlStr.replace("\n", "\\n");
		System.out.println(yamlStr);
//		yamlStr = yamlStr.replace("\\n", "\n");
//		map = (HashMap<String, String>) new Yaml().loadFlat(yamlStr);
//		map = (HashMap<String, String>) new Yaml().load(yamlStr);
//		System.out.println(map);
	}
}
