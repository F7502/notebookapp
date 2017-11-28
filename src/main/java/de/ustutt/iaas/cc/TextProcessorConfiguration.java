package de.ustutt.iaas.cc;

import java.util.List;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

public class TextProcessorConfiguration {

	public static enum Mode {
		local, remoteSingle, remoteMulti, queue, queueError
	};

	public static enum MOM {
		SQS, ActiveMQ
	};

	@NotNull
	public Mode mode;

	@NotNull
	@Size(min = 1)
	public List<String> textProcessors;

	public MOM mom;

	public String activeMQurl;

	public String requestQueueName;

	public String responseQueueName;

}
