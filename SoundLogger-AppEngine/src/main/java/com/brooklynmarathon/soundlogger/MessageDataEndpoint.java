package com.brooklynmarathon.soundlogger;

import com.google.api.server.spi.config.Api;
import com.google.api.server.spi.config.ApiMethod;
import com.google.api.server.spi.config.ApiNamespace;
import com.google.api.server.spi.response.CollectionResponse;
import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.datanucleus.query.JPACursorHelper;

import java.util.List;

import javax.annotation.Nullable;
import javax.inject.Named;
import javax.persistence.EntityExistsException;
import javax.persistence.EntityNotFoundException;
import javax.persistence.EntityManager;
import javax.persistence.Query;

@Api(name = "messagedataendpoint", namespace = @ApiNamespace(ownerDomain = "brooklynmarathon.com", ownerName = "brooklynmarathon.com", packagePath = "soundlogger"))
public class MessageDataEndpoint {

    /**
     * This method lists all the entities inserted in datastore.
     * It uses HTTP GET method and paging support.
     *
     * @return A CollectionResponse class containing the list of all entities
     * persisted and a cursor to the next page.
     */
    @SuppressWarnings({"unchecked", "unused"})
    @ApiMethod(name = "listMessageData")
    public CollectionResponse<MessageData> listMessageData(
            @Nullable @Named("cursor") String cursorString,
            @Nullable @Named("limit") Integer limit) {

        EntityManager mgr = null;
        List<MessageData> execute = null;

        try {
            mgr = getEntityManager();
            Query query = mgr.createQuery("select from MessageData as MessageData");
            Cursor cursor;
            if (cursorString != null && cursorString.trim().length() > 0) {
                cursor = Cursor.fromWebSafeString(cursorString);
                query.setHint(JPACursorHelper.CURSOR_HINT, cursor);
            }

            if (limit != null) {
                query.setFirstResult(0);
                query.setMaxResults(limit);
            }

            execute = (List<MessageData>) query.getResultList();
            cursor = JPACursorHelper.getCursor(execute);
            if (cursor != null) cursorString = cursor.toWebSafeString();

            // Tight loop for fetching all entities from datastore and accomodate
            // for lazy fetch.
            for (MessageData obj : execute) ;
        } finally {
            if (mgr != null) {
                mgr.close();
            }
        }

        return CollectionResponse.<MessageData>builder()
                .setItems(execute)
                .setNextPageToken(cursorString)
                .build();
    }

    /**
     * This method gets the entity having primary key id. It uses HTTP GET method.
     *
     * @  param id the primary key of the java bean.
     * @  return The entity with primary key id.
     */
    /*
    @ApiMethod(name = "getMessageData")
    public MessageData getMessageData(@Named("id") Key id) {
        EntityManager mgr = getEntityManager();
        MessageData messageData = null;
        try {
            messageData = mgr.find(MessageData.class, id);
        } finally {
            mgr.close();
        }
        return messageData;
    }
    */

    /**
     * This inserts a new entity into App Engine datastore. If the entity already
     * exists in the datastore, an exception is thrown.
     * It uses HTTP POST method.
     *
     * @param messageData the entity to be inserted.
     * @return The inserted entity.
     */
    @ApiMethod(name = "insertMessageData")
    public MessageData insertMessageData(MessageData messageData) {
        EntityManager mgr = getEntityManager();
        try {
            if (containsMessageData(messageData)) {
                throw new EntityExistsException("Object already exists");
            }
            mgr.persist(messageData);
        } finally {
            mgr.close();
        }
        return messageData;
    }

    /**
     * This method is used for updating an existing entity. If the entity does not
     * exist in the datastore, an exception is thrown.
     * It uses HTTP PUT method.
     *
     * @param messageData the entity to be updated.
     * @return The updated entity.
     */
    @ApiMethod(name = "updateMessageData")
    public MessageData updateMessageData(MessageData messageData) {
        EntityManager mgr = getEntityManager();
        try {
            if (!containsMessageData(messageData)) {
                throw new EntityNotFoundException("Object does not exist");
            }
            mgr.persist(messageData);
        } finally {
            mgr.close();
        }
        return messageData;
    }

    /**
     * This method removes the entity with primary key id.
     * It uses HTTP DELETE method.
     *
     * @   param id the primary key of the entity to be deleted.
     * @   return The deleted entity.
     */

    /*
    @ApiMethod(name = "removeMessageData")
    public MessageData removeMessageData(@Named("id") Key id) {
        EntityManager mgr = getEntityManager();
        MessageData messageData = null;
        try {
            messageData = mgr.find(MessageData.class, id);
            mgr.remove(messageData);
        } finally {
            mgr.close();
        }
        return messageData;
    }
    */

    private boolean containsMessageData(MessageData messageData) {
        EntityManager mgr = getEntityManager();
        boolean contains = true;
        try {
            MessageData item = mgr.find(MessageData.class, messageData.getKey());
            if (item == null) {
                contains = false;
            }
        } finally {
            mgr.close();
        }
        return contains;
    }

    private static EntityManager getEntityManager() {
        return EMF.get().createEntityManager();
    }

}
