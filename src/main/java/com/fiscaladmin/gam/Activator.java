package com.fiscaladmin.gam;

import java.util.ArrayList;
import java.util.Collection;

import com.fiscaladmin.gam.statementimporter.lib.StatementConsolidator;
import com.fiscaladmin.gam.statementimporter.lib.StatementImporter;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

/**
 * OSGi Bundle Activator for the Statement Importer plugin.
 * <p>
 * This class is responsible for registering and unregistering the
 * {@link StatementImporter} plugin service when the OSGi bundle is
 * started or stopped within the Joget DX platform.
 * <p>
 * The activator is declared in the Maven bundle plugin configuration
 * via the {@code Bundle-Activator} manifest header.
 *
 * @see StatementImporter
 * @see BundleActivator
 */
public class Activator implements BundleActivator {

    /**
     * Collection of service registrations managed by this activator.
     * Used to properly unregister services when the bundle is stopped.
     */
    protected Collection<ServiceRegistration> registrationList;

    /**
     * Called when the OSGi bundle is started.
     * <p>
     * Registers the {@link StatementImporter} plugin as an OSGi service,
     * making it available to the Joget platform as a Process Tool.
     *
     * @param context the bundle context provided by the OSGi framework
     */
    public void start(BundleContext context) {
        registrationList = new ArrayList<ServiceRegistration>();

        // Register plugins here
        registrationList.add(context.registerService(
            StatementImporter.class.getName(), new StatementImporter(), null));
        registrationList.add(context.registerService(
            StatementConsolidator.class.getName(), new StatementConsolidator(), null));
    }

    /**
     * Called when the OSGi bundle is stopped.
     * <p>
     * Unregisters all services that were registered in {@link #start(BundleContext)},
     * ensuring clean shutdown and preventing memory leaks.
     *
     * @param context the bundle context provided by the OSGi framework
     */
    public void stop(BundleContext context) {
        for (ServiceRegistration registration : registrationList) {
            registration.unregister();
        }
    }
}
