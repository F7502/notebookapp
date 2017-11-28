package de.ustutt.iaas.cc.core;

import java.util.Hashtable;
import java.util.UUID;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

import de.ustutt.iaas.cc.TextProcessorConfiguration;

/**
 * NOTE: This implementation is NOT working correctly as it does not correlate
 * request and response messages. This class is only for demonstration purposes!
 * <p>
 * A text processor that uses JMS to send text to a request queue and then waits
 * for the processed text on a response queue. For each text processing request,
 * a unique ID is generated that is later used to correlate responses to their
 * original request.
 * <p>
 * The text processing is realized by (one or more) workers that read from the
 * request queue and write to the response queue.
 * <p>
 * This implementation supports ActiveMQ as well as AWS SQS.
 * 
 * @author hauptfn
 *
 */
public class QueueTextProcessorFlawed implements ITextProcessor {

	private final static Logger logger = LoggerFactory.getLogger(QueueTextProcessor.class);

	private QueueSession session;
	private QueueSender sender;
	private QueueReceiver receiver;

	public QueueTextProcessorFlawed(TextProcessorConfiguration conf) {
		super();
		// initialize JMS stuff (connection to messaging system, sender, message
		// listener, ...)
		try {
			Context jndi = null;
			QueueConnectionFactory conFactory = null;
			// separated handling of ActiveMQ vs. AWS SQS
			switch (conf.mom) {
			case ActiveMQ:
				// initialize JNDI context
				Hashtable<String, String> env = new Hashtable<String, String>();
				env.put("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
				env.put("java.naming.security.principal", "system");
				env.put("java.naming.security.credentials", "manager");
				if (conf.activeMQurl == null) {
					env.put("java.naming.provider.url", "tcp://localhost:61616");
				} else {
					env.put("java.naming.provider.url", conf.activeMQurl);
				}
				env.put("connectionFactoryNames", "ConnectionFactory");
				env.put("queue." + conf.requestQueueName, conf.requestQueueName);
				env.put("queue." + conf.responseQueueName, conf.responseQueueName);
				// create JNDI context (will also read jndi.properties file)
				jndi = new InitialContext(env);
				// connect to messaging system
				conFactory = (QueueConnectionFactory) jndi.lookup("ConnectionFactory");
				break;
			case SQS:
			default:
				// Create the connection factory using the properties file
				// credential provider.
				conFactory = SQSConnectionFactory.builder().withRegion(Region.getRegion(Regions.EU_WEST_1))
						.withAWSCredentialsProvider(new PropertiesFileCredentialsProvider("aws.properties")).build();
				break;
			}
			// create connection
			QueueConnection connection = conFactory.createQueueConnection();
			// create session
			session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			Queue requestQueue = null;
			Queue responseQueue = null;
			// separated handling of ActiveMQ vs. AWS SQS
			switch (conf.mom) {
			case ActiveMQ:
				// lookup queue
				requestQueue = (Queue) jndi.lookup(conf.requestQueueName);
				responseQueue = (Queue) jndi.lookup(conf.responseQueueName);
				break;
			case SQS:
			default:
				requestQueue = session.createQueue(conf.requestQueueName);
				responseQueue = session.createQueue(conf.responseQueueName);
				break;
			}
			// create sender and receiver
			sender = session.createSender(requestQueue);
			receiver = session.createReceiver(responseQueue);
			// start connection (!)
			connection.start();
		} catch (NamingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// message property used for request-response correlation
	private static final String ID_PROP = "id";

	@Override
	public String process(String text) {
		// generate unique ID for request
		String id = UUID.randomUUID().toString();
		try {
			// create and send text message
			Message msg = session.createTextMessage(text);
			msg.setStringProperty(ID_PROP, id);
			logger.debug("Sending message {}", msg.getStringProperty(ID_PROP));
			sender.send(msg);
			// receive message (note: this may be any message, there is no guarantee that
			// this will return the response for the sent request!)
			TextMessage result = (TextMessage) receiver.receive();
			return result.getText();
		} catch (JMSException e) {
			// TODO maybe remove future from work?
			e.printStackTrace();
		}
		// in case of any error, return original (unprocessed) text
		return text;
	}

}
