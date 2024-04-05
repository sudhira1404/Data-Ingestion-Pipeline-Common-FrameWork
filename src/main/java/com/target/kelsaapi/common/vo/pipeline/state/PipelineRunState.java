package com.target.kelsaapi.common.vo.pipeline.state;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name="mdf_ingest_batch_request")
@Getter
@Setter
public class PipelineRunState implements Serializable {
    @Id
    @Column(name = "batch_request_id")
    protected String batchRequestId;

    @Column(name = "batch_request_status")
    protected String batchRequestStatus;

    @Column(name = "created_timestamp")
    @CreationTimestamp
    protected Instant createdTimestamp;

    @Column(name = "source_system")
    protected String sourceSystem;

    @Column(name = "start_date")
    protected String startDate;

    @Column(name = "end_date")
    protected String endDate;

    @Column(name = "landing_file")
    protected String landingFile;

    @Column(name = "source_report_type")
    @Nullable
    private String sourceReportType;

    /**
     * Constructor for v2 requests.
     *
     * @param pipelineState The {@link ApplicationConstants.PipelineStates} corresponding to the current or updated state of the pipeline.
     * @param source The source system from the original request.
     * @param startDate The start date from the original request.
     * @param endDate The end date from the original request.
     * @param filePath The path to where the file should be transferred in HDFS at the conclusion of a successful pipeline run.
     * @param sourceReportType The type of report to be requested from the source for this pipeline from the original request.
     */
    public PipelineRunState(ApplicationConstants.PipelineStates pipelineState,
                            String source,
                            String startDate,
                            String endDate,
                            String filePath,
                            @Nullable String sourceReportType) {
        setBatchRequestId(generateUUID());
        this.batchRequestStatus = pipelineState.toString().toLowerCase();
        this.sourceSystem = source;
        this.startDate = startDate;
        this.endDate = endDate;
        this.landingFile = filePath;
        this.sourceReportType = sourceReportType;
    }

    public PipelineRunState() {

    }

    public void setbatchRequestStatus(ApplicationConstants.PipelineStates pipelineState) {
        this.batchRequestStatus = pipelineState.toString().toLowerCase();
    }

    protected String generateUUID() {
        return UUID.randomUUID().toString();
    }
}
