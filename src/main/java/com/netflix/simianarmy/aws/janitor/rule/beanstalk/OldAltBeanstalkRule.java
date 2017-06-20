package com.netflix.simianarmy.aws.janitor.rule.beanstalk;
import com.netflix.simianarmy.MonkeyCalendar;
import com.netflix.simianarmy.Resource;
import com.netflix.simianarmy.aws.AWSResource;
import com.netflix.simianarmy.aws.janitor.crawler.BeanstalkJanitorCrawler;
import com.netflix.simianarmy.janitor.Rule;
import org.apache.commons.lang.Validate;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class OldAltBeanstalkRule implements Rule{

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(OldAltBeanstalkRule.class);

    private static final String TERMINATION_REASON = "Alt beanstalk older then threshold";

    private final MonkeyCalendar calendar;

    private final int retentionDays;

    private final int ageThreshold;

    public OldAltBeanstalkRule(MonkeyCalendar calendar, int ageThreshold, int retentionDays) {
        Validate.notNull(calendar);
        Validate.isTrue(ageThreshold >= 0);
        Validate.isTrue(retentionDays >= 0);
        this.calendar = calendar;
        this.ageThreshold = ageThreshold;
        this.retentionDays = retentionDays;
    }

    @Override
    public boolean isValid(Resource resource) {
        Validate.notNull(resource);
        if (!"BEANSTALK".equals(resource.getResourceType().name())) {
            return true;
        }
        AWSResource beanstalkResource = (AWSResource) resource;
        if(beanstalkResource.getAdditionalField(BeanstalkJanitorCrawler.INSTANCE_FIELD_IS_ALT_ENV).equalsIgnoreCase("FALSE")){
            LOGGER.info(String.format("Beanstalk %s is not an ATL environment so it is valid",
                    resource.getId()));
            return true;
        }
        DateTime launchTime = new DateTime(resource.getLaunchTime().getTime());
        DateTime now = new DateTime(calendar.now().getTimeInMillis());
        //check for alt here...
        if (now.isAfter(launchTime.plusDays(ageThreshold))) {
            LOGGER.info(String.format("The beanstalk %s was launched for more than %d days",
                    resource.getId(), ageThreshold));
            markResource(resource);
            if(beanstalkResource.getAdditionalField(BeanstalkJanitorCrawler.INSTANCE_FIELD_IS_ALT_ENV).equalsIgnoreCase("TRUE")){
                LOGGER.info(String.format("Returning false for ALT beanstalk %s which was launched for more than %d days",
                        resource.getId(), ageThreshold));
                return false;
            }
        }
        return true;
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
