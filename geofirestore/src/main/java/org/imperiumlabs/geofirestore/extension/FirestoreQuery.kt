package org.imperiumlabs.geofirestore.extension

import com.google.firebase.firestore.CollectionReference
import com.google.firebase.firestore.Query
import org.imperiumlabs.geofirestore.core.GeoHashQuery

/*
 * Given a GeoHashQuery will create a Firestore Query based
 * upon the filterQuery or the collectionReference.
 */
fun GeoHashQuery.makeFirestoreQuery(filterQuery: Query?, collectionRef: CollectionReference) =
        (filterQuery ?: collectionRef)
                .orderBy("g")
                .startAt(this.startValue)
                .endAt(this.endValue)