package pfe_broker.common;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;

/**
 * This class contains the configuration of some logging facilities.
 * 
 * To recapitulate, logging levels are: TRACE, DEBUG, INFO, WARN, ERROR, FATAL.
 * 
 * @author Denis Conan
 * 
 */
public final class Log {
	public static final boolean ON = true;

	public static final String LOGGER_NAME_GEN = "general";
	public static final Logger GEN = LogManager.getLogger(LOGGER_NAME_GEN);

	public static final String LOGGER_NAME_TEST = "test";
	public static final Logger TEST = LogManager.getLogger(LOGGER_NAME_TEST);

	public static final String LOGGER_NAME_APP = "app";
	public static final Logger APP = LogManager.getLogger(LOGGER_NAME_APP);

	public static final String LOGGER_NAME_DB = "db";
	public static final Logger DB = LogManager.getLogger(LOGGER_NAME_DB);
	/*
	 * static configuration, which can be changed by command line options.
	 */
	static {
		setLevel(GEN, Level.WARN);
		setLevel(TEST, Level.WARN);
		setLevel(APP, Level.DEBUG);
		setLevel(DB, Level.DEBUG);
	}

	/**
	 * configures a logger to a level.
	 * 
	 * @param logger the logger.
	 * @param level  the level.
	 */
	public static void setLevel(final Logger logger, final Level level) {
		final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		final var config = ctx.getConfiguration();
		var loggerConfig = config.getLoggerConfig(logger.getName());
		var specificConfig = loggerConfig;
		// We need a specific configuration for this logger,
		// otherwise we would change the level of all other loggers
		// having the original configuration as parent as well
		if (!loggerConfig.getName().equals(logger.getName())) {
			specificConfig = new LoggerConfig(logger.getName(), level, true);
			specificConfig.setParent(loggerConfig);
			config.addLogger(logger.getName(), specificConfig);
		}
		specificConfig.setLevel(level);
		ctx.updateLoggers();
	}

	/**
	 * private constructor to avoid instantiation.
	 */
	private Log() {
	}

}
