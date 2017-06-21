package com.netflix.simianarmy.aws.janitor.rule.alarm;

import com.netflix.simianarmy.MonkeyCalendar;
import com.netflix.simianarmy.Resource;
import com.netflix.simianarmy.aws.AWSResource;
import com.netflix.simianarmy.aws.janitor.crawler.AlarmJanitorCrawler;
import com.netflix.simianarmy.aws.janitor.crawler.BeanstalkJanitorCrawler;
import com.netflix.simianarmy.aws.janitor.rule.beanstalk.OldAltBeanstalkRule;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.netflix.simianarmy.janitor.Rule;

import java.util.Date;

/**
 * Created by Arpan Solanki on 6/21/2017.
 */
public class AlarmWithoutASGRule implements Rule{

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(AlarmWithoutASGRule.class);

    private static final String TERMINATION_REASON = "Alarm with Dimension ASG does not have valid existing ASG";

    private final MonkeyCalendar calendar;

    private final int retentionDays;

    public AlarmWithoutASGRule(MonkeyCalendar calendar, int retentionDays) {
        Validate.notNull(calendar);
        Validate.isTrue(retentionDays >= 0);
        this.calendar = calendar;
        this.retentionDays = retentionDays;
    }

    @Override
    public boolean isValid(Resource resource) {
        Validate.notNull(resource);
        if (!"ALARM".equals(resource.getResourceType().name())) {
            return true;
        }
        AWSResource alarmResource = (AWSResource) resource;

        if(alarmResource.getAdditionalField(AlarmJanitorCrawler.IS_DANGLING_ASG).equalsIgnoreCase("FALSE")){
            LOGGER.info(String.format("Alarm %s is valid", resource.getId()));
            return true;
        }else{
            LOGGER.info(String.format("Alarm %s is NOT valid.marking for deletion", resource.getId()));
            markResource(resource);
        }
        return false;
    }

    private void markResource(Resource resource) {
        if (resource.getExpectedTerminationTime() == null) {
            Date terminationTime = calendar.getBusinessDay(new Date(), retentionDays);
            resource.setExpectedTerminationTime(terminationTime);
            resource.setTerminationReason(TERMINATION_REASON);
        } else {
            LOGGER.info(String.format("Resource %s is already marked as cleanup candidate.", resource.getId()));
        }
    }
}
