package com.target.kelsaapi.common.service.postgres.google.admanager.forecast;

import com.google.common.collect.Lists;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.service.file.LocalFileWriterService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.google.response.admanager.forecast.GamForecastResponse;
import com.target.kelsaapi.common.vo.google.response.admanager.forecast.GamLineItemDeliveryForecastResponse;
import com.target.kelsaapi.common.vo.google.state.admanager.forecast.GamForecastState;
import com.target.kelsaapi.common.vo.google.state.admanager.forecast.GamForecastStateId;
import jakarta.annotation.Nullable;
import jakarta.persistence.EntityManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@Repository
@Service
@Slf4j
public class GamForecastStateService {

    private final GamForecastStateRepository repository;

    private final EntityManager entityManager;

    private final LocalFileWriterService writerService;

    @Autowired
    public GamForecastStateService(GamForecastStateRepository repository, EntityManager entityManager, LocalFileWriterService writerService) {
        this.repository = repository;
        this.entityManager = entityManager;
        this.writerService = writerService;
    }

    public GamForecastLoopStatus getStatuses(String pipelineRunId, String forecastType) {
        return repository.getPipelineAndTypeStatuses(pipelineRunId, forecastType);
    }

    @Transactional(readOnly = true)
    public Path writeAllForecasts(String pipelineRunId, ApplicationConstants.GamForecastTypes forecastType, ApplicationConstants.PipelineStates status, String reportDate) throws IOException {
        String fileName = CommonUtils.generateTempFileRootPath() + pipelineRunId + "_" + forecastType.name().toLowerCase() + "_forecast_report-" + reportDate + ".json";
        try (Stream<GamForecastState> stream = repository.findAllByStartDateAndForecastTypeAndStatus(reportDate, forecastType.name().toLowerCase(), status.name().toLowerCase())) {
            stream.forEach(element -> {
                        writerService.writeLocalFile(Collections.singletonList(element.getResponse()),fileName, false, true);
                        entityManager.detach(element);
                    });
        }
        return Paths.get(fileName);
    }

    @Transactional(readOnly = true)
    public List<Long> getAllLineItemIdsByStartDate(String reportDate, ApplicationConstants.GamForecastTypes forecastType) {
        return repository.getAllLineItemIdsByStartDateAndForecastTypeAndStatus(reportDate, forecastType.name().toLowerCase(), ApplicationConstants.PipelineStates.COMPLETED.name().toLowerCase());
    }

    @Transactional
    public GamForecastStateId initializeNewLineItem(String pipelineRunId, Long lineItem, ApplicationConstants.GamForecastTypes type, String reportStartDate) {
        GamForecastStateId stateId = new GamForecastStateId(pipelineRunId,reportStartDate, lineItem,type.name().toLowerCase());
        repository.saveAndFlush(new GamForecastState(stateId));
        return stateId;
    }

    @Transactional
    public void initializeNewLineItems(String pipelineRunId, List<Long> lineItems, ApplicationConstants.GamForecastTypes type, String reportStartDate) {
        List<GamForecastState> stateList = Lists.newArrayList();
        for (Long id : lineItems) {
            GamForecastStateId stateId = initializeId(id, pipelineRunId, reportStartDate);
            stateList.add(new GamForecastState(stateId));
        }
        repository.saveAllAndFlush(stateList);
    }
    @Transactional
    public void updateStartedState(GamForecastStateId id) {
        repository.updateStartedState(id.getPipelineRunId(),id.getReportStartDate(),id.getLineItemId(),id.getForecastType(),
                ApplicationConstants.PipelineStates.RUNNING.name().toLowerCase());
    }

    @Transactional
    public void updateForecastFinalState(GamForecastResponse response,
                                         GamForecastStateId id) {
        if (response.getFailureReason() != null) {
            repository.updateFailedState(id.getPipelineRunId(),id.getReportStartDate(), id.getLineItemId(), id.getForecastType(),
                    ApplicationConstants.PipelineStates.FAILED.name().toLowerCase(), response.getFailureReason(), response.getAttempts());
        } else {
            repository.updateCompletedState(id.getPipelineRunId(),id.getReportStartDate(), id.getLineItemId(), id.getForecastType(),
                    ApplicationConstants.PipelineStates.COMPLETED.name().toLowerCase(), response.getJsonForecast(), response.getAttempts());
        }
    }

    @Transactional
    public void updateStartedStates(List<Long> ids, String pipelineRunId, String reportStartDate, String forecastType) {
        repository.updateStartedStates(pipelineRunId,reportStartDate,ids,forecastType,
                ApplicationConstants.PipelineStates.RUNNING.name().toLowerCase());
    }

    @Transactional
    public void updateFinalStates(List<Long> idsForecasted,
                                  @Nullable GamLineItemDeliveryForecastResponse response,
                                  @Nullable String exceptionMessage,
                                  String pipelineRunId,
                                  ApplicationConstants.GamForecastTypes forecastType,
                                  String reportStartDate) {
        if (response == null) {
            repository.updateFailedStates(pipelineRunId,reportStartDate,idsForecasted, forecastType.name().toLowerCase(),
                    ApplicationConstants.PipelineStates.FAILED.name().toLowerCase(),
                    Objects.requireNonNullElse(exceptionMessage, "Timed out while requesting forecast."),1);
        } else if (response.getFailureReason() != null) {
            repository.updateFailedStates(pipelineRunId,reportStartDate, Arrays.stream(response.getRequestedLineItems()).boxed().toList(), forecastType.name().toLowerCase(),
                    ApplicationConstants.PipelineStates.FAILED.name().toLowerCase(), response.getFailureReason(),response.getAttempts());
        } else {
            for (Pair<Long,String> idAndResponse : response.getSavableGamForecasts()) {
                repository.updateCompletedState(pipelineRunId,reportStartDate,idAndResponse.getFirst(), forecastType.name().toLowerCase(),
                        ApplicationConstants.PipelineStates.COMPLETED.name().toLowerCase(), idAndResponse.getSecond(), response.getAttempts());
            }
        }
    }

    public GamForecastStateId initializeId(long lineItemId, String pipelineRunId, String reportStartDate) {
        return new GamForecastStateId(
                pipelineRunId, reportStartDate, lineItemId, ApplicationConstants.GamForecastTypes.DELIVERY.name().toLowerCase());
    }

    public Boolean isQueued(String pipelineRunId, String forecastType) {
        return repository.isQueued(pipelineRunId, forecastType);
    }

    @Transactional
    public void purgePriorDaysData(String startDate) {
        repository.deleteAllByReportStartDate(startDate);
    }
}
