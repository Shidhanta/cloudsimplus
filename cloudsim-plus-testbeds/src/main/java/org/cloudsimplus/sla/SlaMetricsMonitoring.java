/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cloudsimplus.sla;

/**
 *
 * @author raysaoliveira
 */
public class SlaMetricsMonitoring {
    private String metric;
    
    public void monitoringMetrics(String metric){
        this.metric = metric;
        
        System.out.println("\n The metric: " + metric + " was violated !!");
    
    }
    
}
