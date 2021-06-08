package addrparser;


import tech.spiro.addrparser.io.file.JSONFileRegionDataInput;
import tech.spiro.addrparser.parser.Location;
import tech.spiro.addrparser.parser.LocationParserEngine;
import tech.spiro.addrparser.parser.ParserEngineException;

import java.io.Serializable;

/**
 * @author ngt on 2021-06-07 18:05
 * @version 1.0
 */
public class MyHandler {

	private static LocationParserEngine engine;

	static {
		// china-region.json文件作为基础数据
		String path = Thread.currentThread().getContextClassLoader().getResource("china-region.json").getPath();
		JSONFileRegionDataInput regionDataInput = new JSONFileRegionDataInput(path);

		// 创建并初始化位置解析引擎，一般配置为全局单例
		engine = new LocationParserEngine(regionDataInput);
		// 初始化，加载数据，比较耗时
		try {
			engine.init();
		} catch (ParserEngineException e) {
			e.printStackTrace();
		}
	}


	public static Location getPoint(double x, double y) {
		Location parse = engine.parse(x, y);
		return parse;
	}

}
