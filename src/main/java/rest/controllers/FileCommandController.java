package rest.controllers;

import avro.commands.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rest.domain.FileDomain;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;

/**
 * File commands REST API
 */
@Path("/files")
public class FileCommandController {

    @Context
    FileDomain domain;

    private static final Logger logger = LogManager.getLogger();

    @GET
    public Response ruok() {
        return Response
                .ok()
                .entity("imok")
                .build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response create(CreateFileCommand cmd) {
        try {
            validate(cmd.getFile());
            domain.create(cmd);
            return Response
                    .ok()
                    .build();
        } catch (Exception e) {
            logger.error(e);
            return Response
                    .serverError()
                    .build();
        }
    }

    @DELETE
    @Path("/{file}")
    public Response remove(@PathParam("file") String file) {
        try {
            validate(file);
            domain.remove(new RemoveFileCommand(file));
            return Response
                    .ok()
                    .build();
        } catch (Exception e) {
            logger.error(e);
            return Response
                    .serverError()
                    .build();
        }
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    public Response update(UpdateFileCommand cmd) {
        try {
            validate(cmd.getFile());
            domain.update(cmd);
            return Response
                    .ok()
                    .build();
        } catch (Exception e) {
            logger.error(e);
            return Response
                    .serverError()
                    .build();
        }
    }

    /**
     * Converts a path string to a {@code Path}
     * @param path
     * @throws InvalidPathException
     *          if the path string cannot be converted to a {@code Path}
     */
    private void validate(CharSequence path) throws InvalidPathException {

        Paths.get(path.toString());
    }
}
