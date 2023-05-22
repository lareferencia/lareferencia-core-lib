package org.lareferencia.core.monitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Service;


@Service
@ManagedResource(objectName = "org.lareferencia:type=LoadingProcessMonitor")
public class LoadingProcessMonitorService {
	
	private int loadedEntities;
	private int duplicatedEntities;
	private int discardedEntities;
	private int processedFiles;
	private int errorFiles;
	
	@Value("${loadingProcessMonitor.isLoadingProcessInProgress:false}")
    private Boolean isLoadingProcessInProgress;
	
	private Map<String, List<String>> running = new HashMap<String, List<String>>();
	private Map<String, List<String>> scheduled = new HashMap<String, List<String>>();
	private Map<String, List<String>> queued = new HashMap<String, List<String>>();
	
	private List<Long> networkIdsWithErrors = new ArrayList<Long>();
	
	@ManagedAttribute(description = "Loading process in progress")
	public boolean isLoadingProcessInProgress() {
		return isLoadingProcessInProgress;
	}

	@ManagedAttribute(description = "Number of individuals of the loaded entities")
	public int getLoadedEntities() {
		return loadedEntities;
	}

	@ManagedAttribute(description = "Number of individuals of the duplicated entities")
	public int getDuplicatedEntities() {
		return duplicatedEntities;
	}

	@ManagedAttribute(description = "Number of individuals from discarded entities")
	public int getDiscardedEntities() {
		return discardedEntities;
	}

	@ManagedAttribute(description = "Number of successfully processed files")
	public int getProcessedFiles() {
		return processedFiles;
	}

	@ManagedAttribute(description = "Number of files processed with error")
	public int getErrorFiles() {
		return errorFiles;
	}
	
	
	@ManagedAttribute(description = "Is Loading Process In Progress?")
	public Boolean getIsLoadingProcessInProgress() {
		return isLoadingProcessInProgress;
	}

	public void setIsLoadingProcessInProgress(Boolean isLoadingProcessInProgress) {
		this.isLoadingProcessInProgress = isLoadingProcessInProgress;
	}

	@ManagedAttribute(description = "How many process is running?")
	public Map<String, List<String>> getRunning() {
		return running;
	}

	public void setRunning(Map<String, List<String>> running) {
		this.running = running;
	}

	@ManagedAttribute(description = "How many process is scheduled?")
	public Map<String, List<String>> getScheduled() {
		return scheduled;
	}

	public void setScheduled(Map<String, List<String>> scheduled) {
		this.scheduled = scheduled;
	}

	@ManagedAttribute(description = "How many process is queued?")
	public Map<String, List<String>> getQueued() {
		return queued;
	}

	public void setQueued(Map<String, List<String>> queued) {
		this.queued = queued;
	}

	public List<Long> getNetworkIdsWithErrors() {
		return networkIdsWithErrors;
	}

	public void setNetworkIdsWithErrors(List<Long> networkIdsWithErrors) {
		this.networkIdsWithErrors = networkIdsWithErrors;
	}

	public void setLoadedEntities(int loadedEntities) {
		this.loadedEntities = loadedEntities;
	}

	public void setDuplicatedEntities(int duplicatedEntities) {
		this.duplicatedEntities = duplicatedEntities;
	}

	public void setDiscardedEntities(int discardedEntities) {
		this.discardedEntities = discardedEntities;
	}

	public void setProcessedFiles(int processedFiles) {
		this.processedFiles = processedFiles;
	}

	public void setErrorFiles(int errorFiles) {
		this.errorFiles = errorFiles;
	}

}
