package com.netflix.simianarmy.aws.janitor.crawler;

import com.amazonaws.services.cloudwatch.model.MetricAlarm;
import com.netflix.simianarmy.Resource;
import com.netflix.simianarmy.ResourceType;
import com.netflix.simianarmy.aws.AWSResource;
import com.netflix.simianarmy.aws.AWSResourceType;
import com.netflix.simianarmy.client.aws.AWSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Arpan Solanki on 6/21/2017.
 */
public class AlarmJanitorCrawler extends AbstractAWSJanitorCrawler{

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(AlarmJanitorCrawler.class);

    public AlarmJanitorCrawler(AWSClient awsClient) {
        super(awsClient);
    }

    /** Alarm is for type ASG but ASG does not exist..then set this field to true **/
    public static final String IS_DANGLING_ASG = "IS_DANGLING_ASG";

    @Override
    public EnumSet<? extends ResourceType> resourceTypes() {
        return EnumSet.of(AWSResourceType.ALARM);
    }

    @Override
    public List<Resource> resources(ResourceType resourceType) {
        if ("ALARM".equals(resourceType.name())) {
            return getAlarmResources();
        }
        return Collections.emptyList();
    }

    @Override
    public List<Resource> resources(String... resourceIds) {
        return getAlarmResources(resourceIds);
    }

    private List<Resource> getAlarmResources(String... names) {
        List<Resource> resources = new LinkedList<Resource>();
        AWSClient awsClient = getAWSClient();
        for(MetricAlarm alarm : awsClient.describeAlarms(names)){
            Resource alarmResource = new AWSResource().withId(alarm.getAlarmName())
                    .withDescription(alarm.getAlarmDescription())
                    .withRegion(getAWSClient().region()).withResourceType(AWSResourceType.ALARM);
            if(isDanglingASGAlarm(alarm)){
                alarmResource.setAdditionalField(IS_DANGLING_ASG, "true");
            }else{
                alarmResource.setAdditionalField(IS_DANGLING_ASG, "false");
            }
            resources.add(alarmResource);
        }
        return resources;
    }

    public boolean isDanglingASGAlarm(MetricAlarm alarm){
        AWSClient awsClient = getAWSClient();
        if(awsClient.isDanglingASGAlarm(alarm)){
            return true;
        }
        return false;
    }
}
