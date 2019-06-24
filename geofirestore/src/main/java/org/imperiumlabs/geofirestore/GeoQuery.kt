package org.imperiumlabs.geofirestore

import com.google.firebase.firestore.*
import org.imperiumlabs.geofirestore.core.GeoHash
import org.imperiumlabs.geofirestore.core.GeoHashQuery
import org.imperiumlabs.geofirestore.extension.makeFirestoreQuery
import org.imperiumlabs.geofirestore.listeners.EventListenerBridge
import org.imperiumlabs.geofirestore.listeners.GeoQueryDataEventListener
import org.imperiumlabs.geofirestore.listeners.GeoQueryEventListener
import org.imperiumlabs.geofirestore.util.GeoUtils

/**
 * A GeoQuery object can be used for geo queries in a given circle. The GeoQuery class is thread safe.
 */
/*
 *
 * Creates a new GeoQuery object centered at the given location and with the given radius.
 * @param geoFirestore The GeoFirestore object this GeoQuery uses
 * @param center The center of this query
 * @param radius The radius of the query, in kilometers. The maximum radius that is
 * supported is about 8587km. If a radius bigger than this is passed we'll cap it.
 */
class GeoQuery(private val geoFirestore: GeoFirestore,
               private var center: GeoPoint,
               radius: Double,
               private val filterQuery: Query? = null) {

    /*
     * Private class used to store all the location previously
     * retrieved from the database along side their data.
     */
    private class LocationInfo(val location: GeoPoint,
                               val inGeoQuery: Boolean,
                               val documentSnapshot: DocumentSnapshot) {
        val geoHash: GeoHash = GeoHash(GeoLocation(location))
    }

    private var radius = GeoUtils.kmToM(GeoUtils.capRadius(radius))
    private val eventListeners = HashSet<GeoQueryDataEventListener>()

    //Map with location infos sorted by document ID
    private val locationInfos = HashMap<String, LocationInfo>()
    //Set of GeoHashQuery already computed
    private var queries: Set<GeoHashQuery>? = null
    //Map of queryListener for each GeoHashQuery
    private val handles = HashMap<GeoHashQuery, ListenerRegistration>()
    //Set of GeoHashQuery that need to be computed
    private val outstandingQueries = HashSet<GeoHashQuery>()

    /*
     * SnapshotListener used to obtain real-time updates from the data in Firestore
     *
     * Every time some document change we loop through all the changes and call the
     * correct method for the child added, changed, removed passing the document.
     */
    private val queryListener = EventListener<QuerySnapshot> { querySnap, ex ->
        if (querySnap != null && ex == null)
            for (docChange in querySnap.documentChanges) when (docChange.type) {
                DocumentChange.Type.ADDED -> childAdded(docChange.document)
                DocumentChange.Type.MODIFIED -> childChanged(docChange.document)
                DocumentChange.Type.REMOVED -> childRemoved(docChange.document)
            }
    }

    /*
     * Notify all the listeners at once.
     */
    private inline fun notifyListeners(crossinline event: (listener: GeoQueryDataEventListener)->Unit) {
        for (listener in this.eventListeners)
            this.geoFirestore.raiseEvent(Runnable { event(listener) })
    }

    /*
     * Return true if a given GeoPoint is within the radius.
     */
    private fun locationIsInQuery(location: GeoPoint) =
            GeoUtils.distance(GeoLocation(location), GeoLocation(center)) <= this.radius

    /*
     * Called every the location info change, notify the correct listener and update
     * the location infos.
     */
    private fun updateLocationInfo(documentSnapshot: DocumentSnapshot, location: GeoPoint) {
        val documentID = documentSnapshot.id
        val oldInfo = this.locationInfos[documentID]

        val isNew = oldInfo == null
        val changedLocation = oldInfo != null && oldInfo.location != location
        val wasInQuery = oldInfo != null && oldInfo.inGeoQuery
        val isInQuery = this.locationIsInQuery(location)

        when {
            //document added
            ((isNew || !wasInQuery) && isInQuery) -> this.notifyListeners { it.onDocumentEntered(documentSnapshot, location) }
            //document changed and moved
            (!isNew && isInQuery) -> this.notifyListeners {
                if (changedLocation)
                    it.onDocumentMoved(documentSnapshot, location)
                it.onDocumentChanged(documentSnapshot, location)
            }
            //document exited
            (wasInQuery && !isInQuery) -> this.notifyListeners { it.onDocumentExited(documentSnapshot) }
        }

        this.locationInfos[documentID] = LocationInfo(location, this.locationIsInQuery(location), documentSnapshot)
    }

    private fun geoHashQueriesContainGeoHash(geoHash: GeoHash): Boolean {
        if (this.queries == null)
            return false
        for (query in this.queries!!)
            if (query.containsGeoHash(geoHash))
                return true
        return false
    }

    private fun hasListeners() = this.eventListeners.isNotEmpty()

    private fun canFireReady() = this.outstandingQueries.isEmpty()

    private fun checkAndFireReady() {
        if (canFireReady())
            this.notifyListeners { it.onGeoQueryReady() }
    }

    /*
    // TODO: 11/06/19 correct this comment
     * given a firestore query and his respecting hashquery we exec the firestore one
     * if it's successfully done remove the query from the outstandings and fire ready
     * if there is an exception call the onGeoQueryError method for every listener
     */
    private fun addValueToReadyListener(firestoreQuery: Query, query: GeoHashQuery) {
        firestoreQuery.get()
                .addOnSuccessListener {
                    synchronized(this@GeoQuery) {
                        this.outstandingQueries.remove(query)
                        this.checkAndFireReady()
                    }
                }
                .addOnFailureListener { e ->
                    synchronized(this@GeoQuery) { this.notifyListeners { it.onGeoQueryError(e) } }
                }
    }

    /*
     * Setup the queries considering all the old and outstanding queries.
     */
    private fun setupQueries() {
        val oldQueries = if (queries != null) queries!! else HashSet()
        val newQueries = GeoHashQuery.queriesAtLocation(GeoLocation(center), radius)
        this.queries = newQueries

        //Remove every old query that is not in the new query
        for (query in oldQueries)
            if (!newQueries.contains(query)) {
                handles[query]?.remove()
                handles.remove(query)
                outstandingQueries.remove(query)
            }

        //Add every query that is not present in the old queries
        for (query in newQueries)
            if (!oldQueries.contains(query)) {
                outstandingQueries.add(query)
                val firestoreQuery = query.makeFirestoreQuery(filterQuery, geoFirestore.collectionReference)

                //create the ListenerRegistration from the firestoreQuery for child added, changed, removed
                handles[query] =  firestoreQuery.addSnapshotListener(queryListener)

                addValueToReadyListener(firestoreQuery, query)
            }

        //loop every location infos and update them
        for (info in this.locationInfos) {
            val oldLocationInfo = info.value
            updateLocationInfo(oldLocationInfo.documentSnapshot, oldLocationInfo.location)
        }

        // remove locations that are not part of the geo query anymore
        val iterator = this.locationInfos.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (!this.geoHashQueriesContainGeoHash(entry.value.geoHash)) {
                iterator.remove()
            }
        }

        checkAndFireReady()
    }

    /*
     * Clear all the queries currently in the class.
     */
    private fun reset() {
        for(handle in handles.values)
            handle.remove()

        this.handles.clear()
        this.queries = null
        this.locationInfos.clear()
        this.outstandingQueries.clear()
    }

    /*
     * Called every time a document is added
     */
    private fun childAdded(documentSnapshot: DocumentSnapshot) {
        GeoFirestore.getLocationValue(documentSnapshot)?.let {
            this.updateLocationInfo(documentSnapshot, it)
        }
    }

    /*
     * Called every time a document is changed
     */
    private fun childChanged(documentSnapshot: DocumentSnapshot) {
        GeoFirestore.getLocationValue(documentSnapshot)?.let {
            this.updateLocationInfo(documentSnapshot, it)
        }
    }

    /*
     * Called every time a document is removed
     */
    private fun childRemoved(documentSnapshot: DocumentSnapshot) {
        val documentID = documentSnapshot.id
        val info = this.locationInfos[documentID]

        if (info != null)
            this.geoFirestore.getRefForDocumentID(documentID).get()
                    .addOnSuccessListener {
                        synchronized(this@GeoQuery) {
                            val location = GeoFirestore.getLocationValue(it)
                            val hash = if (location != null) GeoHash(GeoLocation(location)) else null
                            if (hash == null || !this.geoHashQueriesContainGeoHash(hash)) {
                                val locInfo = this.locationInfos.remove(documentID)
                                if (locInfo != null && locInfo.inGeoQuery)
                                    this.notifyListeners { it.onDocumentExited(locInfo.documentSnapshot) }
                            }
                        }
                    }
    }

    /**
     * Adds a new GeoQueryEventListener to this GeoQuery.
     *
     * @throws IllegalArgumentException If this listener was already added
     *
     * @param listener The listener to add
     */
    @Synchronized
    fun addGeoQueryEventListener(listener: GeoQueryEventListener) {
        addGeoQueryDataEventListener(EventListenerBridge(listener))
    }

    /**
     * Adds a new GeoQueryEventListener to this GeoQuery.
     *
     * @throws IllegalArgumentException If this listener was already added
     *
     * @param listener The listener to add
     */
    @Synchronized
    fun addGeoQueryDataEventListener(listener: GeoQueryDataEventListener) {
        if (eventListeners.contains(listener))
            throw IllegalArgumentException("Added the same listener twice to a GeoQuery!")
        eventListeners.add(listener)
        if (this.queries == null) {
            this.setupQueries()
        } else {
            for (entry in this.locationInfos) {
                val info = entry.value
                if (info.inGeoQuery)
                    this.geoFirestore.raiseEvent(Runnable { listener.onDocumentEntered(info.documentSnapshot, info.location) })
            }
            if (this.canFireReady())
                this.geoFirestore.raiseEvent(Runnable { listener.onGeoQueryReady() })
        }
    }

    /**
     * Removes an event listener.
     *
     * @throws IllegalArgumentException If the listener was removed already or never added
     *
     * @param listener The listener to remove
     */
    @Synchronized
    fun removeGeoQueryEventListener(listener: GeoQueryEventListener) {
        removeGeoQueryEventListener(EventListenerBridge(listener))
    }

    /**
     * Removes an event listener.
     *
     * @throws IllegalArgumentException If the listener was removed already or never added
     *
     * @param listener The listener to remove
     */
    @Synchronized
    fun removeGeoQueryEventListener(listener: GeoQueryDataEventListener) {
        if (!eventListeners.contains(listener))
            throw IllegalArgumentException("Trying to remove listener that was removed or not added!")
        eventListeners.remove(listener)
        if (!this.hasListeners())
            reset()
    }

    /**
     * Removes all event listeners from this GeoQuery.
     */
    @Synchronized
    fun removeAllListeners() {
        eventListeners.clear()
        reset()
    }

    /**
     * Returns the current center of this query.
     * @return The current center
     */
    @Synchronized
    fun getCenter() = center

    /**
     * Sets the new center of this query and triggers new events if necessary.
     * @param center The new center
     */
    @Synchronized
    fun setCenter(center: GeoPoint) {
        this.center = center
        if (this.hasListeners())
            this.setupQueries()
    }

    /**
     * Returns the radius of the query, in kilometers.
     * @return The radius of this query, in kilometers
     */
    @Synchronized
    fun getRadius() = GeoUtils.mToKm(radius)

    /**
     * Sets the radius of this query, in kilometers, and triggers new events if necessary.
     * @param radius The radius of the query, in kilometers. The maximum radius that is
     * supported is about 8587km. If a radius bigger than this is passed we'll cap it.
     */
    @Synchronized
    fun setRadius(radius: Double) {
        this.radius = GeoUtils.kmToM(GeoUtils.capRadius(radius))
        if (this.hasListeners())
            this.setupQueries()
    }

    /**
     * Sets the center and radius (in kilometers) of this query, and triggers new events if necessary.
     * @param center The new center
     * @param radius The radius of the query, in kilometers. The maximum radius that is
     * supported is about 8587km. If a radius bigger than this is passed we'll cap it.
     */
    @Synchronized
    fun setLocation(center: GeoPoint, radius: Double) {
        this.center = center
        this.radius = GeoUtils.kmToM(GeoUtils.capRadius(radius))
        if (this.hasListeners())
            this.setupQueries()
    }
}