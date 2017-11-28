package de.ustutt.iaas.cc;

import java.util.List;
import java.util.UUID;

import javax.ws.rs.client.Client;

import org.apache.commons.lang3.StringUtils;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

import de.thomaskrille.dropwizard_template_config.TemplateConfigBundle;
import de.thomaskrille.dropwizard_template_config.TemplateConfigBundleConfiguration;
import de.ustutt.iaas.cc.core.DatabaseNotebookDAO;
import de.ustutt.iaas.cc.core.GoogleDatastoreNotebookDAO;
import de.ustutt.iaas.cc.core.INotebookDAO;
import de.ustutt.iaas.cc.core.INotesDB;
import de.ustutt.iaas.cc.core.ITextProcessor;
import de.ustutt.iaas.cc.core.LocalTextProcessor;
import de.ustutt.iaas.cc.core.QueueTextProcessor;
import de.ustutt.iaas.cc.core.QueueTextProcessorFlawed;
import de.ustutt.iaas.cc.core.RemoteTextProcessor;
import de.ustutt.iaas.cc.core.RemoteTextProcessorMulti;
import de.ustutt.iaas.cc.core.SimpleNotebookDAO;
import de.ustutt.iaas.cc.resources.NotebookResource;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.jdbi.bundles.DBIExceptionsBundle;
import io.dropwizard.migrations.MigrationsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;

public class NotebookappApplication extends Application<NotebookappConfiguration> {

	private final static Logger logger = LoggerFactory.getLogger(NotebookappApplication.class);

	public static void main(final String[] args) throws Exception {
		new NotebookappApplication().run(args);
	}

	// ID of app instance
	private String myID;

	@Override
	public String getName() {
		return "notebookapp";
	}

	@Override
	public void initialize(final Bootstrap<NotebookappConfiguration> bootstrap) {
		// enables access to static assets (html, css, js, ...)
		bootstrap.addBundle(new AssetsBundle("/assets", "/", "index.html"));
		// enables swagger UI
		bootstrap.addBundle(new SwaggerBundle<NotebookappConfiguration>() {
			protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(NotebookappConfiguration configuration) {
				return configuration.swaggerBundleConfiguration;
			}
		});
		// enables unwrapping of SQL and DBI database exceptions
		bootstrap.addBundle(new DBIExceptionsBundle());
		// enables DB migrations (http://www.liquibase.org/)
		bootstrap.addBundle(new MigrationsBundle<NotebookappConfiguration>() {
			@Override
			public DataSourceFactory getDataSourceFactory(NotebookappConfiguration configuration) {
				return configuration.getDataSourceFactory();
			}
		});
		// enables use of Freemaker templates in config.yml
		bootstrap.addBundle(new TemplateConfigBundle(new TemplateConfigBundleConfiguration().charset(Charsets.UTF_8)
				.outputPath("config_generated.yml")
				.addCustomProvider(new PropertiesFileTemplateConfigVariablesProvider("db.properties", "dbprops"))));
	}

	@Override
	public void run(final NotebookappConfiguration configuration, final Environment environment) {
		// get service instance ID from config file
		myID = configuration.serviceInstanceID;
		// if not set, generate random service instance ID
		if (StringUtils.isBlank(myID)) {
			logger.debug("setting random service instance ID");
			myID = UUID.randomUUID().toString();
		}
		logger.info("Service Instance ID is: " + myID);

		// apply data storage configuration
		INotebookDAO dao = null;
		switch (configuration.notesDatabaseConfiguration.mode) {
		case tmp:
			logger.info("Using local notes storage (non-persistent in-memory)");
			dao = new SimpleNotebookDAO();
			break;
		case jdbc:
			if (configuration.getDataSourceFactory() != null) {
				logger.info("Using JDBC notes storage ({})", configuration.getDataSourceFactory().getUrl());
				final DBIFactory factory = new DBIFactory();
				final DBI jdbi = factory.build(environment, configuration.getDataSourceFactory(), "jdbi");
				final INotesDB dbAccess = jdbi.onDemand(INotesDB.class);
				dao = new DatabaseNotebookDAO(dbAccess);
			} else {
				logger.warn("No JDBC configuration found, defaulting to tmp");
				dao = new SimpleNotebookDAO();
			}
			break;
		case gcds:
			logger.info("Using Google Cloud Datastore notes storage");
			dao = new GoogleDatastoreNotebookDAO();
			break;
		default:
			logger.warn("Unknown or empty notes DB mode ({}), defaulting to tmp",
					configuration.notesDatabaseConfiguration.mode);
			dao = new SimpleNotebookDAO();
			break;
		}

		// apply text processor configuration
		ITextProcessor tp = null;
		switch (configuration.textProcessorConfiguration.mode) {
		case local:
			logger.info("Using local text processor");
			tp = new LocalTextProcessor("local-" + myID);
			break;
		case remoteSingle:
			String ep = configuration.textProcessorConfiguration.textProcessors.get(0);
			logger.info("Using single remote text processor at {}", ep);
			JerseyClientConfiguration jcfs = configuration.getJerseyClientConfiguration();
			jcfs.setGzipEnabled(false);
			final Client clients = new JerseyClientBuilder(environment).using(jcfs).build(getName());
			tp = new RemoteTextProcessor(ep, clients);
			break;
		case remoteMulti:
			List<String> eps = configuration.textProcessorConfiguration.textProcessors;
			logger.info("Using multiple remote text processors: {}", eps);
			JerseyClientConfiguration jcfm = configuration.getJerseyClientConfiguration();
			jcfm.setGzipEnabled(false);
			final Client clientm = new JerseyClientBuilder(environment).using(jcfm).build(getName());
			tp = new RemoteTextProcessorMulti(eps, clientm);
			break;
		case queue:
			logger.info("Using queue text processor reading from {} and writing to {}",
					configuration.textProcessorConfiguration.requestQueueName,
					configuration.textProcessorConfiguration.responseQueueName);
			tp = new QueueTextProcessor(configuration.textProcessorConfiguration);
			break;
		case queueError:
			logger.info("Using FLAWED queue text processor reading from {} and writing to {}",
					configuration.textProcessorConfiguration.requestQueueName,
					configuration.textProcessorConfiguration.responseQueueName);
			tp = new QueueTextProcessorFlawed(configuration.textProcessorConfiguration);
			break;
		default:
			logger.warn("Unknown or empty text processor mode ({}), defaulting to local",
					configuration.textProcessorConfiguration.mode);
			tp = new LocalTextProcessor("local-" + myID);
			break;
		}

		if (dao != null && tp != null) {
			// create and register resource class
			final NotebookResource root = new NotebookResource(dao, tp);
			environment.jersey().register(root);
		} else {
			logger.error("DAO or text processor is null, cannot register resource.");
			// TODO exit with error?
		}
	}

}
