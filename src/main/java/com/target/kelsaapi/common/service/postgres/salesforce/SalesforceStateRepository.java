package com.target.kelsaapi.common.service.postgres.salesforce;

import com.target.kelsaapi.common.vo.salesforce.state.Salesforce;
import com.target.kelsaapi.common.vo.salesforce.state.SalesforceId;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository("salesforceStateRepository")
public interface SalesforceStateRepository extends CrudRepository<Salesforce, SalesforceId> {

    @Query(value = "select sf.attributes from Salesforce as sf where sf.reportType = :reportType")
    List<String> getSalesforceAttributesByReportType(@Param("reportType") String reportType);

}
