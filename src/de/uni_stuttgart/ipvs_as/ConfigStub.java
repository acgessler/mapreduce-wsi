package de.uni_stuttgart.ipvs_as;

import java.util.Properties;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

/**
 * Upon servlet startup, load the mapreduce-wsi configuration file and make it
 * available in the "config" attribute of the servlet context.
 * 
 * According to http://stackoverflow.com/questions/19610234/ this is a good way
 * of loading a configuration file once and allowing JAX-WS endpoint instances
 * running in a servlet to access it. A whole class for this is not verbose
 * after all. If anybody knows a better way, please enlighten me.
 */
@WebListener
public class ConfigStub implements ServletContextListener {

	public static final String CONFIG_FILE_NAME = "mapreduce-wsi-config.xml";

	public void contextInitialized(ServletContextEvent sce) {
		// Load static configuration
		final Properties properties = new Properties();
		try {
			properties.loadFromXML(sce.getServletContext().getResourceAsStream(
					"/WEB-INF/" + CONFIG_FILE_NAME));
		} catch (Exception e) {
			sce.getServletContext().log(
					"CRITICAL: mapreduce-wsi config file not found");
			return;
		}

		sce.getServletContext().log(
				"Loaded mapreduce-wsi configuration from " + CONFIG_FILE_NAME);

		// Store the bean in application context
		ServletContext context = sce.getServletContext();
		context.setAttribute("config", properties);
	}

	public void contextDestroyed(ServletContextEvent sce) {
		ServletContext context = sce.getServletContext();
		context.removeAttribute("config");
	}
}
