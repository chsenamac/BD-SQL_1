use master
go

if exists(select *  from sysdatabases where name = 'Obligatorio')
begin
	drop database Obligatorio
end
go

create database Obligatorio
go

use Obligatorio
go

-- Tablas
create table Usuario
(
	cedula int not null,
	nombreCompleto varchar(50),
	nombreUsuario varchar(20) primary key
)
go

create table UsuarioActividad
(
	nombreUsuarioActividad varchar(20) not null foreign key references Usuario(nombreUsuario),
	actividad varchar(50)

	primary key(nombreUsuarioActividad, actividad)
)
go 

create table Mensaje
(
	numeroInterno int identity(0, 1),
	fechaGenerado datetime,
	asunto varchar(50),
	texto varchar(200),

	usuarioEmisor varchar(20) not null foreign key references Usuario(nombreUsuario),
	usuarioReceptor varchar(20) not null foreign key references Usuario(nombreUsuario),

	primary key(numeroInterno)
)
go

create table TipoMensaje
(
	codigoInterno varchar(3) primary key check(codigoInterno like '[A-Z][A-Z][A-Z]'),
	nombre varchar(20)
)
go

create table Comun
(
	numeroInternoMC int foreign key references Mensaje(numeroInterno),
	codigoInternoTM varchar(3) foreign key references TipoMensaje(codigoInterno), --falta el check?

	primary key(numeroInternoMC)
)
go

create table Privado
(
	numeroInternoMP int foreign key references Mensaje(numeroInterno),
	fechaCaducidad datetime,
	
	primary key(numeroInternoMP)
)
go

-- Procedimientos almacenados solicitados

-- A)
create procedure EliminarMensaje
@numeroInterno int
as
begin
	if not exists(select * from Mensaje where numeroInterno = @numeroInterno)
		return -1
		
	begin transaction
		if exists(select * from Comun where numeroInternoMC = @numeroInterno)
		begin
			Delete from Comun
			where Comun.numeroInternoMC = @numeroInterno

			Delete from Mensaje
			where Mensaje.numeroInterno = @numeroInterno
		end
		else
		begin
			Delete from Privado 
			where Privado.numeroInternoMP = @numeroInterno

			Delete from Mensaje 
			where Mensaje.numeroInterno = @numeroInterno
		end
		
	if @@error <> 0
	begin
		rollback transaction
		return -2
	end

	commit transaction
end
go

-- B)

create procedure ModificarTipoMensaje
@codigoInterno varchar(3),
@nombre varchar(20)
as
begin 
	if not exists(select* from TipoMensaje where codigoInterno = @codigoInterno)
		return -1
	
	if exists(select * from Comun where codigoInternoTM = @codigoInterno) 
		return -2
	
	update TipoMensaje
	set nombre = @nombre
	where codigoInterno = @codigoInterno
	
	if @@error = 0
		return 1
end
go

-- C)
create procedure AgregarMensajeComun
@asunto varchar(50),
@texto varchar(200),
@usuarioEmisor varchar(20),
@usuarioReceptor varchar(20),
@codigoInternoMensaje varchar(3)
as
begin
	declare @numeroInternoGenerado int

	if not exists(select * from Usuario where nombreUsuario = @usuarioEmisor)
		return -1
	
	if not exists(select * from Usuario where nombreUsuario = @usuarioReceptor)
		return -2

	if not exists(select * from TipoMensaje where codigoInterno = @codigoInternoMensaje)
		return -3

	begin transaction
		
		insert Mensaje(fechaGenerado, asunto, texto, usuarioEmisor, usuarioReceptor)
		values(getdate(), @asunto, @texto, @usuarioEmisor, @usuarioReceptor)
		
		if @@error <> 0
		begin
			rollback transaction
			return -4
		end
		
		set @numeroInternoGenerado = @@identity
		insert Comun(numeroInternoMC, codigoInternoTM) values(@numeroInternoGenerado, @codigoInternoMensaje)
		
		if @@error <> 0
		begin
			rollback transaction
			return -4
		end
		 	
	commit transaction

	return @numeroInternoGenerado
end
go

-- D)
create procedure ListadoMensajesPrivados
as
begin
	select Mensaje.numeroInterno, Mensaje.fechaGenerado, Mensaje.asunto, Mensaje.texto,
		   Emisor.nombreCompleto, Receptor.nombreCompleto, Privado.fechaCaducidad
	from Mensaje inner join Usuario Emisor
	on Mensaje.usuarioEmisor = Emisor.nombreUsuario
	inner join Usuario Receptor
	on Mensaje.usuarioEmisor = Receptor.nombreUsuario
	inner join Privado
	on Mensaje.numeroInterno = Privado.numeroInternoMP
	where Privado.fechaCaducidad >= dateadd(day,-2,getdate())
end
go

-- E)
create procedure ContarMensajesEnviadosUsuario
as
begin
	select Usuario.nombreCompleto, count(Mensaje.usuarioEmisor) as [cantidad]
	from Mensaje right join Usuario
	on Mensaje.usuarioEmisor = Usuario.nombreUsuario
	group by Mensaje.usuarioEmisor, Usuario.nombreCompleto
	order by cantidad desc
end
go

-- F)
create procedure TotalMensajesComunesUsuarioTipo
as
begin 
	select Mensaje.usuarioReceptor, Comun.codigoInternoTM, count(*) Cantidad
	from Mensaje inner join Comun 
	on Mensaje.numeroInterno = Comun.numeroInternoMC 
	group by Mensaje.usuarioReceptor, Comun.codigoInternoTM
	order by Mensaje.usuarioReceptor
end
go


-- Procedimientos almacenados para ingreso de datos de prueba
create procedure AltaUsuario
@cedula int,
@nombreCompleto varchar(50),
@nombreUsuario varchar(20)
as
begin
	if exists(select * from Usuario where nombreUsuario = @nombreUsuario)
		return -1
	
	insert Usuario(cedula, nombreCompleto, nombreUsuario) 
	values(@cedula, @nombreCompleto, @nombreUsuario)
	
	insert UsuarioActividad(nombreUsuarioActividad, actividad)
	values(@nombreUsuario, '')
	
	if @@error = 0
		return 1
	
end
go

create procedure AltaUsuarioActividad
@nombreUsuario varchar(20),
@actividad varchar(50)
as
begin
		if not exists(select * from Usuario inner join UsuarioActividad 
					  on Usuario.nombreUsuario = UsuarioActividad.nombreUsuarioActividad
					  where Usuario.nombreUsuario = @nombreUsuario and UsuarioActividad.nombreUsuarioActividad = @nombreUsuario)
			return -1					
	
		if exists(select * from UsuarioActividad where nombreUsuarioActividad = @nombreUsuario and actividad = '')
			update UsuarioActividad
			set actividad = @actividad
			where nombreUsuarioActividad = @nombreUsuario
		else
			insert UsuarioActividad(nombreUsuarioActividad, actividad) values(@nombreUsuario, @actividad)
		
		if @@error = 0
			return 1
end
go

create procedure AltaTipoMensaje
@codigoInterno varchar(3),
@nombre varchar(20)
as
begin
	if exists(select * from TipoMensaje where codigoInterno = @codigoInterno)
		return -1
	
	insert TipoMensaje(codigoInterno, nombre) 
	values(@codigoInterno, @nombre)
	
	if @@error = 0
		return 1
end
go


create procedure AgregarMensajePrivado
@asunto varchar(50),
@texto varchar(200),
@usuarioEmisor varchar(20),
@usuarioReceptor varchar(20)
as
begin
	declare @numeroInternoGenerado int

	if not exists(select * from Usuario where nombreUsuario = @usuarioEmisor)
		return -1
	
	if not exists(select * from Usuario where nombreUsuario = @usuarioReceptor)
		return -2

	insert Mensaje(fechaGenerado, asunto, texto, usuarioEmisor, usuarioReceptor)
	values(getdate(), @asunto, @texto, @usuarioEmisor, @usuarioReceptor)
	
	set @numeroInternoGenerado = @@identity

	insert Privado(numeroInternoMP, fechaCaducidad) values(@numeroInternoGenerado, dateadd(day, 2, getdate()))
	
	if @@error = 0
		return @numeroInternoGenerado
end
go



--PRUEBAS
exec AltaUsuario 31899838, 'Christiams Sena Machado', 'csenamac'
exec AltaUsuario 49855585, 'Sofia Sena Altez', 'ssenaalt'
exec AltaUsuario 55597016, 'Agustin Sena Altez', 'asenaalt'
exec AltaUsuario 51586140, 'Candela Hernandez', 'chernandez'

exec AltaUsuarioActividad 'csenamac', 'Pescar'
exec AltaUsuarioActividad 'csenamac', 'leer'
exec AltaUsuarioActividad 'csenamac', 'Programar'

exec AltaTipoMensaje 'URG', 'Urgente'
exec AltaTipoMensaje 'EVT', 'Evento'
exec AltaTipoMensaje 'ITC', 'Invitacion'

exec AgregarMensajeComun 'Prueba mensaje comun 01', 'Mensaje comun 01', 'csenamac', 'asenaalt', 'URG'
exec AgregarMensajeComun 'Prueba mensaje comun 02', 'Mensaje comun 02', 'csenamac', 'asenaalt', 'URG'
exec AgregarMensajeComun 'Prueba mensaje comun 03', 'Mensaje comun 03', 'csenamac', 'asenaalt', 'ITC'
exec AgregarMensajeComun 'Prueba mensaje comun 04', 'Mensaje comun 04', 'csenamac', 'asenaalt', 'URG'
exec AgregarMensajeComun 'Prueba mensaje comun 05', 'Mensaje comun 05', 'csenamac', 'asenaalt', 'EVT'
exec AgregarMensajeComun 'Prueba mensaje comun 06', 'Mensaje comun 06', 'csenamac', 'asenaalt', 'ITC'
exec AgregarMensajeComun 'Prueba mensaje comun 07', 'Mensaje comun 07', 'csenamac', 'asenaalt', 'URG'
exec AgregarMensajeComun 'Prueba mensaje comun 08', 'Mensaje comun 08', 'csenamac', 'asenaalt', 'EVT'

exec AgregarMensajePrivado 'Prueba mensaje privado 001', 'Mensaje privado 001', 'ssenaalt', 'chernandez'
exec AgregarMensajePrivado 'Prueba mensaje privado 002', 'Mensaje privado 002', 'ssenaalt', 'chernandez'
exec AgregarMensajePrivado 'Prueba mensaje privado 003', 'Mensaje privado 003', 'ssenaalt', 'chernandez'
exec AgregarMensajePrivado 'Prueba mensaje privado 004', 'Mensaje privado 004', 'ssenaalt', 'chernandez'

--Datos de las tablas
select * from Usuario
select * from UsuarioActividad
select * from Mensaje
select * from TipoMensaje
select * from Comun
select * from Privado

-- Pruebas de los SP solicitados
-- A)
select  * from Mensaje
exec EliminarMensaje 2
select * from Mensaje

-- B)
exec ModificarTipoMensaje 'ITC', 'Cumpleaños'

-- C)
exec AgregarMensajeComun 'Hola mundo!', 'Obligatorio BD 2022', 'ssenaalt', 'asenaalt', 'URG'

-- D)
exec ListadoMensajesPrivados

-- E)
exec ContarMensajesEnviadosUsuario

-- F)
exec TotalMensajesComunesUsuarioTipo