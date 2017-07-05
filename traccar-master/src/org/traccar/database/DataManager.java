/*
 * Copyright 2012 - 2016 Anton Tananaev (anton.tananaev@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.database;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.naming.InitialContext;
import javax.sql.DataSource;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.exception.LiquibaseException;
import liquibase.resource.FileSystemResourceAccessor;
import liquibase.resource.ResourceAccessor;

import org.traccar.Config;
import org.traccar.Context;
import org.traccar.helper.Log;
import org.traccar.model.Device;
import org.traccar.model.DevicePermission;
import org.traccar.model.Event;
import org.traccar.model.Group;
import org.traccar.model.GroupPermission;
import org.traccar.model.Position;
import org.traccar.model.Server;
import org.traccar.model.User;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.HttpsURLConnection;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class DataManager implements IdentityManager {

    private static final long DEFAULT_REFRESH_DELAY = 300;

    private final Config config;

    private DataSource dataSource;

    private final long dataRefreshDelay;

    private final ReadWriteLock devicesLock = new ReentrantReadWriteLock();
    private final Map<Long, Device> devicesById = new HashMap<>();
    private final Map<String, Device> devicesByUniqueId = new HashMap<>();
    private long devicesLastUpdate;

    private final ReadWriteLock groupsLock = new ReentrantReadWriteLock();
    private final Map<Long, Group> groupsById = new HashMap<>();
    private long groupsLastUpdate;
    
    private ArrayList<Map<String, String>> dispositivosEstadosZonas;
    private ArrayList<Map<String, Integer>> dispositivosContadorZonas;
    private ArrayList<Map<String, String>> dispositivosEstadosZonasConfirmados;
    
    private final int MAX_REPETICIONES = 3;

    public DataManager(Config config) throws Exception {
        this.config = config;

        initDatabase();
        initDatabaseSchema();

        dataRefreshDelay = config.getLong("database.refreshDelay", DEFAULT_REFRESH_DELAY) * 1000;
        
        if(cargarArrayDispositivos()){
            System.out.println("Array de dispositivos cargado.");
        } else {
            dispositivosEstadosZonas = new ArrayList<Map<String, String>>();
            dispositivosEstadosZonas.add(null);
            guardarArrayDispositivos();
            
            dispositivosContadorZonas = new ArrayList<Map<String, Integer>>();
            dispositivosContadorZonas.add(null);
            
            dispositivosEstadosZonasConfirmados = new ArrayList<Map<String, String>>();
            dispositivosEstadosZonasConfirmados.add(null);
            
            // Añadir a cartodb el usuario admin admin
            System.out.println("-----------> Añadiendo al administrador !!!!!!");
            String urlParameters = "q=INSERT INTO users" 
                            + "(cartodb_id, username, email, password, salt)"
                            + " VALUES (1, 'admin', 'admin@admin.es', 'BEF6EBB00275D6C83C5300D69CBA5548A8FD0BC07E6F6A44', 'ABB6183F383F884C254B301A95A86897E90B904B04B5A49A')&api_key=bb027343ceb82dece775db749f966f81c9e58763";
            doPostCartoDB(urlParameters);
        }
        
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    private void initDatabase() throws Exception {

        String jndiName = config.getString("database.jndi");

        if (jndiName != null) {

            dataSource = (DataSource) new InitialContext().lookup(jndiName);

        } else {

            String driverFile = config.getString("database.driverFile");
            if (driverFile != null) {
                URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
                Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                method.setAccessible(true);
                method.invoke(classLoader, new File(driverFile).toURI().toURL());
            }

            String driver = config.getString("database.driver");
            if (driver != null) {
                Class.forName(driver);
            }

            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setDriverClassName(config.getString("database.driver"));
            hikariConfig.setJdbcUrl(config.getString("database.url"));
            hikariConfig.setUsername(config.getString("database.user"));
            hikariConfig.setPassword(config.getString("database.password"));
            hikariConfig.setConnectionInitSql(config.getString("database.checkConnection", "SELECT 1"));
            hikariConfig.setIdleTimeout(600000);

            int maxPoolSize = config.getInteger("database.maxPoolSize");

            if (maxPoolSize != 0) {
                hikariConfig.setMaximumPoolSize(maxPoolSize);
            }

            dataSource = new HikariDataSource(hikariConfig);

        }
    }

    private void updateDeviceCache(boolean force) throws SQLException {
        boolean needWrite;
        devicesLock.readLock().lock();
        try {
            needWrite = force || System.currentTimeMillis() - devicesLastUpdate > dataRefreshDelay;
        } finally {
            devicesLock.readLock().unlock();
        }

        if (needWrite) {
            devicesLock.writeLock().lock();
            try {
                if (force || System.currentTimeMillis() - devicesLastUpdate > dataRefreshDelay) {
                    devicesById.clear();
                    devicesByUniqueId.clear();
                    for (Device device : getAllDevices()) {
                        devicesById.put(device.getId(), device);
                        devicesByUniqueId.put(device.getUniqueId(), device);
                    }
                    devicesLastUpdate = System.currentTimeMillis();
                }
            } finally {
                devicesLock.writeLock().unlock();
            }
        }
    }

    @Override
    public Device getDeviceById(long id) {
        boolean forceUpdate;
        devicesLock.readLock().lock();
        try {
            forceUpdate = !devicesById.containsKey(id);
        } finally {
            devicesLock.readLock().unlock();
        }

        try {
            updateDeviceCache(forceUpdate);
        } catch (SQLException e) {
            Log.warning(e);
        }

        devicesLock.readLock().lock();
        try {
            return devicesById.get(id);
        } finally {
            devicesLock.readLock().unlock();
        }
    }

    @Override
    public Device getDeviceByUniqueId(String uniqueId) throws SQLException {
        boolean forceUpdate;
        devicesLock.readLock().lock();
        try {
            forceUpdate = !devicesByUniqueId.containsKey(uniqueId) && !config.getBoolean("database.ignoreUnknown");
        } finally {
            devicesLock.readLock().unlock();
        }

        updateDeviceCache(forceUpdate);

        devicesLock.readLock().lock();
        try {
            return devicesByUniqueId.get(uniqueId);
        } finally {
            devicesLock.readLock().unlock();
        }
    }

    private void updateGroupCache(boolean force) throws SQLException {
        boolean needWrite;
        groupsLock.readLock().lock();
        try {
            needWrite = force || System.currentTimeMillis() - groupsLastUpdate > dataRefreshDelay;
        } finally {
            groupsLock.readLock().unlock();
        }

        if (needWrite) {
            groupsLock.writeLock().lock();
            try {
                if (force || System.currentTimeMillis() - groupsLastUpdate > dataRefreshDelay) {
                    groupsById.clear();
                    for (Group group : getAllGroups()) {
                        groupsById.put(group.getId(), group);
                    }
                    groupsLastUpdate = System.currentTimeMillis();
                }
            } finally {
                groupsLock.writeLock().unlock();
            }
        }
    }

    public Group getGroupById(long id) {
        boolean forceUpdate;
        groupsLock.readLock().lock();
        try {
            forceUpdate = !groupsById.containsKey(id);
        } finally {
            groupsLock.readLock().unlock();
        }

        try {
            updateGroupCache(forceUpdate);
        } catch (SQLException e) {
            Log.warning(e);
        }

        groupsLock.readLock().lock();
        try {
            return groupsById.get(id);
        } finally {
            groupsLock.readLock().unlock();
        }
    }

    private String getQuery(String key) {
        String query = config.getString(key);
        if (query == null) {
            Log.info("Query not provided: " + key);
        }
        return query;
    }

    private void initDatabaseSchema() throws SQLException, LiquibaseException {

        if (config.hasKey("database.changelog")) {

            ResourceAccessor resourceAccessor = new FileSystemResourceAccessor();

            Database database = DatabaseFactory.getInstance().openDatabase(
                    config.getString("database.url"),
                    config.getString("database.user"),
                    config.getString("database.password"),
                    null, resourceAccessor);

            Liquibase liquibase = new Liquibase(
                    config.getString("database.changelog"), resourceAccessor, database);

            liquibase.clearCheckSums();

            liquibase.update(new Contexts());
        }
    }

    public User login(String email, String password) throws SQLException {
        User user = QueryBuilder.create(dataSource, getQuery("database.loginUser"))
                .setString("email", email)
                .executeQuerySingle(User.class);
        if (user != null && user.isPasswordValid(password)) {
            return user;
        } else {
            return null;
        }
    }

    public Collection<User> getUsers() throws SQLException {
        return QueryBuilder.create(dataSource, getQuery("database.selectUsersAll"))
                .executeQuery(User.class);
    }

    public User getUser(long userId) throws SQLException {
        return QueryBuilder.create(dataSource, getQuery("database.selectUser"))
                .setLong("id", userId)
                .executeQuerySingle(User.class);
    }

    /***************************** Modificaciones a partir de aqui **************************************/

    public void addUser(User user) throws SQLException {
        long id;
        user.setId(id=QueryBuilder.create(dataSource, getQuery("database.insertUser"), true)
                .setObject(user)
                .executeUpdate());
        addUserCartoDB(id, user);
    }
    
    public void addUserCartoDB(long id, User user){
        //INSERT INTO users (name, email, hashedPassword, salt, admin, map, distanceUnit, speedUnit, latitude, longitude, zoom, twelveHourFormat)
        String urlParameters = "q=INSERT INTO users" 
                        + "(cartodb_id, username, email, password, salt)"
                        + " VALUES ("
                        + id + ", '"
                        + user.getName()+ "', '"
                        + user.getEmail()+ "', '"
                        + user.getHashedPassword() + "', '"
                        + user.getSalt()
                        + "')&api_key=bb027343ceb82dece775db749f966f81c9e58763";
        doPostCartoDB(urlParameters);
    }
    /***********************************************************************************************************/

    public void updateUser(User user) throws SQLException {
        QueryBuilder.create(dataSource, getQuery("database.updateUser"))
                .setObject(user)
                .executeUpdate();
        if (user.getHashedPassword() != null) {
            QueryBuilder.create(dataSource, getQuery("database.updateUserPassword"))
                .setObject(user)
                .executeUpdate();
        }
    }

    public void removeUser(long userId) throws SQLException {
        QueryBuilder.create(dataSource, getQuery("database.deleteUser"))
                .setLong("id", userId)
                .executeUpdate();
    }

    public Collection<DevicePermission> getDevicePermissions() throws SQLException {
        return QueryBuilder.create(dataSource, getQuery("database.selectDevicePermissions"))
                .executeQuery(DevicePermission.class);
    }

    public Collection<GroupPermission> getGroupPermissions() throws SQLException {
        return QueryBuilder.create(dataSource, getQuery("database.selectGroupPermissions"))
                .executeQuery(GroupPermission.class);
    }

    public Collection<Device> getAllDevices() throws SQLException {
        return QueryBuilder.create(dataSource, getQuery("database.selectDevicesAll"))
                .executeQuery(Device.class);
    }

    public Collection<Device> getDevices(long userId) throws SQLException {
        Collection<Device> devices = new ArrayList<>();
        for (long id : Context.getPermissionsManager().getDevicePermissions(userId)) {
            devices.add(getDeviceById(id));
        }
        return devices;
    }

    /***************************** Modificaciones a partir de aqui **************************************/

    public void addDevice(Device device) throws SQLException {
        long id;
        device.setId(id=QueryBuilder.create(dataSource, getQuery("database.insertDevice"), true)
                .setObject(device)
                .executeUpdate());
        updateDeviceCache(true);
        addDeviceCartoDB(device, id);
        dispositivosEstadosZonas.add((int)id, null);
        dispositivosContadorZonas.add((int)id, null);
        dispositivosEstadosZonasConfirmados.add((int)id, null);
        guardarArrayDispositivos();
        System.out.println("El identificador del dispositivo recien añadido es: " + id);
    }
    
    public void addDeviceCartoDB(Device device, long id){
        String urlParameters = "q=INSERT INTO devices" 
                        + "(cartodb_id, name, uniqueid)"
                        + " VALUES ("
                        + id + ", '"
                        + device.getName()+ "', '"
                        + device.getUniqueId()
                        + "')&api_key=bb027343ceb82dece775db749f966f81c9e58763";
        doPostCartoDB(urlParameters);
    }

    public void updateDevice(Device device) throws SQLException {
        updateDeviceCartoDB(device);
        QueryBuilder.create(dataSource, getQuery("database.updateDevice"))
                .setObject(device)
                .executeUpdate();
        updateDeviceCache(true);
    }
    
    public void updateDeviceCartoDB(Device device){
        //UPDATE devices SET name = :name, uniqueId = :uniqueId WHERE id = :id;
        String urlParameters = "q=UPDATE devices SET "
                + "name = '" + device.getName()+ "', "
                + "uniqueid = '" + device.getUniqueId()
                + "' WHERE cartodb_id="+ device.getId()
                + "&api_key=bb027343ceb82dece775db749f966f81c9e58763";  
        doPostCartoDB(urlParameters);
    }

    public void updateDeviceStatus(Device device) throws SQLException {
        updateDeviceStatusCartoDB(device);
        QueryBuilder.create(dataSource, getQuery("database.updateDeviceStatus"))
                .setObject(device)
                .executeUpdate();
        Device cachedDevice = getDeviceById(device.getId());
        cachedDevice.setStatus(device.getStatus());
        cachedDevice.setMotion(device.getMotion());
    }
    
    public void updateDeviceStatusCartoDB(Device device){
        // UPDATE devices SET status = :status, lastUpdate = :lastUpdate WHERE id = :id;
        String urlParameters = "q=UPDATE devices SET "
                + "status = '" + device.getStatus()+ "', "
                + "lastupdate = '" + device.getLastUpdate()
                + "' WHERE cartodb_id="+ device.getId()
                + "&api_key=bb027343ceb82dece775db749f966f81c9e58763";
        doPostCartoDB(urlParameters);
    }

    public void removeDevice(long deviceId) throws SQLException {
        deleteDeviceCartoDB(deviceId);
        QueryBuilder.create(dataSource, getQuery("database.deleteDevice"))
                .setLong("id", deviceId)
                .executeUpdate();
        updateDeviceCache(true);
    }
    
    public void deleteDeviceCartoDB(long deviceId){
        //DELETE FROM devices WHERE id = :id;
        String urlParameters = "q=DELETE FROM devices WHERE cartodb_id="
                +  deviceId
                + "&api_key=bb027343ceb82dece775db749f966f81c9e58763";
        doPostCartoDB(urlParameters);
    }
    
    public void linkDevice(long userId, long deviceId) throws SQLException {
        QueryBuilder.create(dataSource, getQuery("database.linkDevice"))
                .setLong("userId", userId)
                .setLong("deviceId", deviceId)
                .executeUpdate();
        linkDeviceCartoDB(userId, deviceId);
    }
    
    public void linkDeviceCartoDB(long userId, long deviceId){
        //INSERT INTO users_devices (userId, deviceId) VALUES (:userId, :deviceId);
        String urlParameters = "q=INSERT INTO users_devices (deviceid, userid) VALUES("
                +  deviceId + "," + userId + ")"
                + "&api_key=bb027343ceb82dece775db749f966f81c9e58763";
        doPostCartoDB(urlParameters);
    }

    /***********************************************************************************************************/

    public void unlinkDevice(long userId, long deviceId) throws SQLException {
        QueryBuilder.create(dataSource, getQuery("database.unlinkDevice"))
                .setLong("userId", userId)
                .setLong("deviceId", deviceId)
                .executeUpdate();
        unlinkDeviceCartoDB(userId, deviceId);
    }
    
    public void unlinkDeviceCartoDB(long userId, long deviceId) throws SQLException {
        //DELETE FROM user_device WHERE userId = :userId AND deviceId = :deviceId;
        String urlParameters = "q=DELETE FROM users_devices WHERE deviceid="
                +  deviceId + " AND userid=" + userId
                + "&api_key=bb027343ceb82dece775db749f966f81c9e58763";
        doPostCartoDB(urlParameters);
    }

    public Collection<Group> getAllGroups() throws SQLException {
        return QueryBuilder.create(dataSource, getQuery("database.selectGroupsAll"))
                .executeQuery(Group.class);
    }

    public Collection<Group> getGroups(long userId) throws SQLException {
        Collection<Group> groups = new ArrayList<>();
        for (long id : Context.getPermissionsManager().getGroupPermissions(userId)) {
            groups.add(getGroupById(id));
        }
        return groups;
    }

    private void checkGroupCycles(Group group) {
        groupsLock.readLock().lock();
        try {
            Set<Long> groups = new HashSet<>();
            while (group != null) {
                if (groups.contains(group.getId())) {
                    throw new IllegalArgumentException("Cycle in group hierarchy");
                }
                groups.add(group.getId());
                group = groupsById.get(group.getGroupId());
            }
        } finally {
            groupsLock.readLock().unlock();
        }
    }

    public void addGroup(Group group) throws SQLException {
        checkGroupCycles(group);
        group.setId(QueryBuilder.create(dataSource, getQuery("database.insertGroup"), true)
                .setObject(group)
                .executeUpdate());
        updateGroupCache(true);
    }

    public void updateGroup(Group group) throws SQLException {
        checkGroupCycles(group);
        QueryBuilder.create(dataSource, getQuery("database.updateGroup"))
                .setObject(group)
                .executeUpdate();
        updateGroupCache(true);
    }

    public void removeGroup(long groupId) throws SQLException {
        QueryBuilder.create(dataSource, getQuery("database.deleteGroup"))
                .setLong("id", groupId)
                .executeUpdate();
    }

    public void linkGroup(long userId, long groupId) throws SQLException {
        QueryBuilder.create(dataSource, getQuery("database.linkGroup"))
                .setLong("userId", userId)
                .setLong("groupId", groupId)
                .executeUpdate();
    }

    public void unlinkGroup(long userId, long groupId) throws SQLException {
        QueryBuilder.create(dataSource, getQuery("database.unlinkGroup"))
                .setLong("userId", userId)
                .setLong("groupId", groupId)
                .executeUpdate();
    }

    public Collection<Position> getPositions(long deviceId, Date from, Date to) throws SQLException {
        return QueryBuilder.create(dataSource, getQuery("database.selectPositions"))
                .setLong("deviceId", deviceId)
                .setDate("from", from)
                .setDate("to", to)
                .executeQuery(Position.class);
    }

    /***************************** Modificaciones a partir de aqui **************************************/

    public void addPosition(Position position) throws SQLException {
        long id;
        position.setId(id=QueryBuilder.create(dataSource, getQuery("database.insertPosition"), true)
                .setDate("now", new Date())
                .setObject(position)
                .executeUpdate());
        addPositionCartoDB(position, id);
        Comprobacion(position);
    }

    public void addPositionCartoDB(Position position, long id){
        String urlParameters = "q=INSERT INTO positions"
                + "(cartodb_id,the_geom,address,altitude,attributes,course,deviceid,devicetime,fixtime,latitude,longitude,protocol,speed,valid, servertime)"
                + " VALUES ("+ id
                + ", " + "ST_GeomFromText('POINT(" + position.getLongitude() + " " + position.getLatitude() + ")', 4326)"
                + ", '" + position.getAddress() + "',"
                + position.getAltitude() + ","
                + "'"+ position.getAttributes() + "',"
                + position.getCourse() + ","
                + position.getDeviceId() + ","
                + "'"+ position.getDeviceTime() + "',"
                + "'"+ position.getFixTime() + "',"
                + position.getLatitude() + ","
                + position.getLongitude() + ","
                + "'"+  position.getProtocol() + "',"
                + position.getSpeed() + ","
                + "'"+ position.getValid() + "',"
                + "'" + new Date()
                + "')&api_key=bb027343ceb82dece775db749f966f81c9e58763";
        doPostCartoDB(urlParameters);
    }
  
    String ObtenerNombreZona(String id){
        String nombre="";
        String urlParameters = "q=SELECT name FROM zonas WHERE cartodb_id = " + id;
	String respuesta = doPostCartoDB(urlParameters);
        try {
            JSONObject json = new JSONObject(respuesta);
            JSONArray rows = json.getJSONArray("rows");
            nombre = rows.getJSONObject(0).getString("name");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return nombre;
    }
    
    ArrayList<String> ObtenerIdsNotificaciones(int id){
        ArrayList<String> destinos = new ArrayList<String>();
        //consultar a carto el id del dispositivo a notificar
        String urlParameters = "q=SELECT idnotification FROM users WHERE cartodb_id IN (SELECT userid FROM users_devices WHERE deviceid="+ id + ")";
        String respuesta = doPostCartoDB(urlParameters);

        try {
            JSONObject json = new JSONObject(respuesta);
            int num = json.getInt("total_rows");
            JSONArray rows = json.getJSONArray("rows");
            for(int i=0; i < num; i++){
                if(!rows.getJSONObject(i).isNull("idnotification")){
                    destinos.add(rows.getJSONObject(i).getString("idnotification"));
                }    
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return destinos;
    }

    String ObtenerNombreDisp(int id){
        Device device = getDeviceById(id);
        String nombre = device.getName();
        return nombre;
    }
    
    void Comprobacion(Position position){
        //Obtencion del arbol con los estados actuales de las zonas del dispositivo
        Map<String, String> arbolZonasActual = obtenerArbolEstadoActual(position);
        
        int idDisp = (int)position.getDeviceId();

        if(dispositivosEstadosZonas.get(idDisp) == null){
            //No habia ningun estado anterior almacenado con el que comparar
            System.out.println("Creando el primer arbol para el dispositivo...");
            dispositivosEstadosZonas.set(idDisp, arbolZonasActual);
            dispositivosEstadosZonasConfirmados.set(idDisp, arbolZonasActual);
            //inicializar arbol de repeticiones
            inicializarContadoresPorId(idDisp, arbolZonasActual);
        } else {
            //Hay un estado anterior con el que podemos comparar para ver si el estado del dispositivo ha cambiado respecto alguna de sus zonas
            if (dispositivosEstadosZonas.get(idDisp).equals(arbolZonasActual)) {
                System.out.println("Son iguales");
                
                //COMPROBAR SI ALGUNA ZONA TIENE EL CONTADOR DE REPETICIONES DISTINTO DE 0

                //Arbol con el numero de repeticiones de cada zona
                Map<String, Integer> arbolRepeticiones = dispositivosContadorZonas.get(idDisp);
                
                System.out.println("Buscamos alguna zona con contador de repeticiones != 0");
                //Recorrido del arbol en busca de alguna zona con repeticiones != 0
                for(Map.Entry<String, Integer> zona: arbolRepeticiones.entrySet()){
                    Integer numRepeticiones = zona.getValue();
                    if(numRepeticiones != 0){
                        System.out.println("Num repeticiones != 0: " + numRepeticiones);
                        if(numRepeticiones == MAX_REPETICIONES){
                            System.out.println("Num repeticiones MAX: " + zona.getKey());
                            //Variables para el envio de notificaciones
                            String mensaje;
                            ArrayList<String> destinos = ObtenerIdsNotificaciones(idDisp);
                            String nombreDisp = ObtenerNombreDisp(idDisp);

                            String v_nuevo = arbolZonasActual.get(zona.getKey());
                            String n = ObtenerNombreZona(zona.getKey());

                            if (v_nuevo.equals("0")) {
                                System.out.println("El dispositivo ha pasado de estar dentro a fuera.");
                                mensaje =nombreDisp + " ha salido de "+ n +".";
                            } else {
                                System.out.println("El dispositivo ha pasado de estar fuera a dentro.");
                                mensaje = nombreDisp + " ha entrado en " + n + ".";
                            }
                            for(int cont=0; cont < destinos.size(); cont++){
                                doPostNotification(destinos.get(cont), mensaje);
                            } 
                            
                            //Introducimos este estado como estado confirmado
                            dispositivosEstadosZonasConfirmados.set(idDisp, arbolZonasActual);

                            //Reseteamos el contador de esta zona
                            arbolRepeticiones.put(zona.getKey(), 0);
                        } else {
                            System.out.println("Incremento num Repeticiones: " + zona.getKey());
                            //Aumentamos el numero de repeticiones para seguir dandole el margen de posible error
                            numRepeticiones = numRepeticiones+1;
                            arbolRepeticiones.put(zona.getKey(), numRepeticiones);
                        }
                    }
                }

                //Actualizamos el arbol de repeticiones de ese dispositivo
                dispositivosContadorZonas.set(idDisp, arbolRepeticiones);
            } else {
                System.out.println("NO son iguales");
                boolean mismoEstado = false;
                
                if(dispositivosEstadosZonasConfirmados.get(idDisp).equals(arbolZonasActual)){
                    System.out.println("El estado actual es el mismo que el confirmado");
                    mismoEstado = true;
                } else {
                    for(Map.Entry<String, String> zona: arbolZonasActual.entrySet()){
                        if ( dispositivosEstadosZonasConfirmados.get(idDisp).containsKey(zona.getKey()) && dispositivosEstadosZonas.get(idDisp).containsKey(zona.getKey()) ) {
                            String v_nuevo = zona.getValue();
                            String v_viejo = dispositivosEstadosZonas.get(idDisp).get(zona.getKey());
                            String v_confirmado = dispositivosEstadosZonasConfirmados.get(idDisp).get(zona.getKey());
                            if (!v_nuevo.equals(v_viejo) && v_nuevo.equals(v_confirmado)) {
                                mismoEstado = true;
                            }
                        }
                    }
                }
                
                inicializarContadoresPorId(idDisp, arbolZonasActual);


                for(Map.Entry<String, String> zona: arbolZonasActual.entrySet()){
                    System.out.println("Entra FOR");
                    if ( dispositivosEstadosZonas.get(idDisp).containsKey(zona.getKey()) ) {
                        String v_nuevo = zona.getValue();
                        String v_viejo = dispositivosEstadosZonas.get(idDisp).get(zona.getKey());
                        if (!v_nuevo.equals(v_viejo) && !mismoEstado) {
                            System.out.println("Contador repeticiones incrememntado: " + zona.getKey());
                            dispositivosContadorZonas.get(idDisp).put(zona.getKey(), 1);
                        }
                    }
                }
                //Actualizacion del estado actual de este dispositivo respecto todas sus zonas
                dispositivosEstadosZonas.set(idDisp, arbolZonasActual);
                System.out.println("Nuevo estado actualizado: " + arbolZonasActual.toString());
            }
        }
	
    }
    
    private Map<String, String> obtenerArbolEstadoActual(Position position){
        Map<String, String> arbolZonas = new TreeMap<String, String>();
        
        //SELECT CompruebaAreas(deviceid, long, lat)
        String urlParameters = "q=SELECT CompruebaAreasNew("+
                position.getDeviceId() + ", " +
                position.getLongitude() + ", " +
                position.getLatitude() + ")&api_key=bb027343ceb82dece775db749f966f81c9e58763";
        String respuesta = doPostCartoDB(urlParameters);
                
        try {
            JSONObject json = new JSONObject(respuesta);
            JSONArray rows = json.getJSONArray("rows");
            String array = rows.getJSONObject(0).getString("compruebaareasnew");

            String[] zonas = array.split(";");
            for(int i=0; i < zonas.length; i++){
                String kv[] = zonas[i].split(",");
                arbolZonas.put(kv[0], kv[1]);
                System.out.println("Zona " + kv[0] + " con valor " + kv[1] );
            }  
        } catch (JSONException e) {
            e.printStackTrace();
	}
        
        return arbolZonas;
    }
    
    private void inicializarContadoresPorId(int id, Map<String, String> arbolZonas){
        System.out.println("Inicializando contadores de :" + id);
        Map<String, Integer> arbolRepeticiones = new TreeMap<String, Integer>();

        for(Map.Entry<String, String> zona: arbolZonas.entrySet()){
            arbolRepeticiones.put(zona.getKey(), 0);
        }
        dispositivosContadorZonas.set(id, arbolRepeticiones);
        System.out.println("Inicializado el arbol de repeticiones: " + arbolRepeticiones.toString());
    }
    
    public void updateLatestPosition(Position position) throws SQLException {
        updateLatestPositionCartoDB(position);
        QueryBuilder.create(dataSource, getQuery("database.updateLatestPosition"))
                .setDate("now", new Date())
                .setObject(position)
                .executeUpdate();
        Device device = getDeviceById(position.getDeviceId());
        device.setPositionId(position.getId());
    }

    public void updateLatestPositionCartoDB(Position position){
        //UPDATE devices SET positionId = :id WHERE id = :deviceId;
        String urlParameters = "q=UPDATE devices SET last_latitude = " + position.getLatitude() 
                + ", last_longitude = " + position.getLongitude() 
                + ", the_geom = ST_GeomFromText('POINT(" + position.getLongitude() + " " + position.getLatitude() + ")', 4326)"
                + " WHERE cartodb_id=" + position.getDeviceId() 
                + "&api_key=bb027343ceb82dece775db749f966f81c9e58763";
        doPostCartoDB(urlParameters);
    }
    /***********************************************************************************************************/

    public Collection<Position> getLatestPositions() throws SQLException {
        return QueryBuilder.create(dataSource, getQuery("database.selectLatestPositions"))
                .executeQuery(Position.class);
    }

    public Server getServer() throws SQLException {
        return QueryBuilder.create(dataSource, getQuery("database.selectServers"))
                .executeQuerySingle(Server.class);
    }

    public void updateServer(Server server) throws SQLException {
        QueryBuilder.create(dataSource, getQuery("database.updateServer"))
                .setObject(server)
                .executeUpdate();
    }

    public Event getEvent(long eventId) throws SQLException {
        return QueryBuilder.create(dataSource, getQuery("database.selectEvent"))
                .setLong("id", eventId)
                .executeQuerySingle(Event.class);
    }

    public void addEvent(Event event) throws SQLException {
        event.setId(QueryBuilder.create(dataSource, getQuery("database.insertEvent"), true)
                .setObject(event)
                .executeUpdate());
    }

    public Collection<Event> getEvents(long deviceId, String type, Date from, Date to) throws SQLException {
        return QueryBuilder.create(dataSource, getQuery("database.selectEvents"))
                .setLong("deviceId", deviceId)
                .setString("type", type)
                .setDate("from", from)
                .setDate("to", to)
                .executeQuery(Event.class);
    }

    public Collection<Event> getLastEvents(long deviceId, String type, int interval) throws SQLException {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, -interval);
        Date to = calendar.getTime();
        return getEvents(deviceId, type, new Date(), to);
    }
    
    
    

    /***************************** Modificaciones a partir de aqui **************************************/

    public boolean cargarArrayDispositivos() {
        try {
            FileInputStream fis = new FileInputStream("disp_zonas.map");
            ObjectInputStream ois = new ObjectInputStream(fis);
            this.dispositivosEstadosZonas = (ArrayList<Map<String, String>>) ois.readObject();
            this.dispositivosEstadosZonas.add(1, null);
            this.dispositivosEstadosZonas.add(2, null);
            this.dispositivosEstadosZonasConfirmados = new ArrayList<Map<String, String>>();
            this.dispositivosEstadosZonasConfirmados.addAll(this.dispositivosEstadosZonas);
            this.dispositivosContadorZonas = new ArrayList<Map<String, Integer>>();
            for(int i=0; i<this.dispositivosEstadosZonas.size(); i++){
                this.dispositivosContadorZonas.add(i, new TreeMap<String, Integer>());
                if(this.dispositivosEstadosZonas.get(i) != null){
                    for(Map.Entry<String, String> zona: this.dispositivosEstadosZonas.get(i).entrySet()){
                        this.dispositivosContadorZonas.get(i).put(zona.getKey(), 0);
                    }
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }

    }

    public void guardarArrayDispositivos() {
        try {
            FileOutputStream fos = new FileOutputStream("disp_zonas.map");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(this.dispositivosEstadosZonas);
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    
    
    public String doPostCartoDB(String urlParameters){
        try {
            String url = "https://cextreme.cartodb.com/api/v2/sql";
            URL obj = new URL(url);
            HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();
            
            //add reuqest header
            con.setRequestMethod("POST");
            
            // Send post request
            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(urlParameters);
            wr.flush();
            wr.close();
            
            int responseCode = con.getResponseCode();
            System.out.println("\nSending 'POST' request to URL : " + url);
            System.out.println("Post parameters : " + urlParameters);
            System.out.println("Response Code : " + responseCode);
            
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();
            
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            
            //print result
            System.out.println(response.toString());
            return response.toString();
        } catch (MalformedURLException ex) {
            Logger.getLogger(DataManager.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(DataManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "error";
    }
    
    
    public void doPostNotification(String dest, String mensaje){
        System.out.println("PRUEBA POST NOTIFICACION");
	//String apikey = "key=AAAANDMNXEg:APA91bG5U1Dat9T-jLTDpB1khB7gQ2ht8aRghS0F43eFKaJDV_ZBa1B2I3o6q4-I466waPBdGMs0rdGBLrqm0S2qMg0rxbF1bNM4A_wPL64cVfsvPY7hQ0qA8YE28UGLTlVSc7OEOHow";
        //String apikey = "key=AIzaSyBVeqskWRVAHGS1GXmX-36ACwnvuhBbXRY";
        String apikey = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiI3MzRmM2U0YS00MTMxLTQyNDQtYjEwYi0zMGUzY2YwYjZiZTYifQ.UjyvPpgy4xFBoXFfL-AC65Edq4jHqUsT27eRF-KMzAw";
	
        //destinatario es lo que cambiara, por tanto traccar debe consultarlo a ionic y el mensaje ira en funcion del cambio
        //dest = "f3abRXtS5Fg:APA91bHpi01QFMbdKhJKqvMOmC7UL180NIBNQ6gFGpWi1-bDLrR2eeW9SSwaiuMNZPt5qeGK5gAxvBoPNvCeTRwf-5AH_-0ko_f9SjtMxwByxwWopDpJHz3wcyhqwq5FGXHxu-fAjKmT";
	//mensaje = "Cambio en alguna zona";
        
	try {
           //String url = "https://fcm.googleapis.com/fcm/send";
           //String urlParameters = "{\"to\": \"" + dest + "\",\"data\": {\"title\": \"Notificación\", \"message\": \"" + mensaje + "\"}}";
           String url = "https://api.ionic.io/push/notifications";
           String urlParameters= "{" +
                                    "\"tokens\": [\""+ dest + "\"]," +
                                    "\"profile\": \"geocontrolnotif\"," +
                                    "\"notification\": {" +
                                      "\"title\": \"GeoControl\"," +
                                      "\"message\": \"" + mensaje + "\"," +
                                      "\"android\": {" +
                                        "\"title\": \"GeoControl\"," +
                                        "\"message\": \"" + mensaje + "\"" +
                                      "}," +
                                      "\"ios\": {" +
                                        "\"title\": \"GeoControl\"," +
                                        "\"message\": \"" + mensaje + "\"" +
                                      "}" +
                                    "}" +
                                  "}";
           
           URL obj = new URL(url);
           HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();
           
           //add request header
           con.setRequestMethod("POST");
           con.setRequestProperty("Authorization", apikey);
           con.setRequestProperty("Content-Type", "application/json");
            
           // Send post request
           con.setDoOutput(true);
           DataOutputStream wr = new DataOutputStream(con.getOutputStream());
           wr.writeBytes(urlParameters);
           wr.flush();
           wr.close();
           
           int responseCode = con.getResponseCode();
           System.out.println("\nSending 'POST' request to URL : " + url);
           System.out.println("Post parameters : " + urlParameters);
           System.out.println("Response Code : " + responseCode);
           
           BufferedReader in = new BufferedReader(
                   new InputStreamReader(con.getInputStream()));
           String inputLine;
           StringBuffer response = new StringBuffer();
           
           while ((inputLine = in.readLine()) != null) {
               response.append(inputLine);
           }
           in.close();
           
           //print result
           System.out.println(response.toString());
       } catch (MalformedURLException ex) {} 
        catch (IOException ex) {}
    }
    /***********************************************************************************************************/

}
