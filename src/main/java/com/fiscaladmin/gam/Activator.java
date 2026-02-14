package com.fiscaladmin.gam;

import java.util.ArrayList;
import java.util.Collection;

import com.fiscaladmin.gam.statementimporter.lib.StatementImporter;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

public class Activator implements BundleActivator {

    protected Collection<ServiceRegistration> registrationList;

    public void start(BundleContext context) {
        registrationList = new ArrayList<ServiceRegistration>();

        // Register plugin here
        registrationList.add(context.registerService(
            StatementImporter.class.getName(), new StatementImporter(), null));
    }

    public void stop(BundleContext context) {
        for (ServiceRegistration registration : registrationList) {
            registration.unregister();
        }
    }
}
