package pedidos360.analytics.dto;

import java.io.Serializable;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public class PedidoVentaDto implements Serializable {

    private static final long serialVersionUID = 1L;

    @NotNull(message = "El ID del pedido no puede ser nulo")
    private Long pedidoId;

    private String usuarioId;

    @NotBlank(message = "El nombre del cliente es obligatorio")
    private String cliente;

    @NotBlank(message = "El localId es obligatorio")
    private String localId;

    @NotBlank(message = "El localNombre es obligatorio")
    private String localNombre;

    private String categoria;

    @NotBlank(message = "Los productos no pueden estar vacíos")
    private String productos;

    @NotNull
    @Min(1)
    private Integer cantidadTotal;

    @NotNull
    @DecimalMin("0.01")
    private Double totalPrecio;

    private String estado;
    private String fechaHora;
    private String correlationId;
    private boolean simularErrorKafka;

    public PedidoVentaDto() {}

    public Long getPedidoId() { return pedidoId; }
    public void setPedidoId(Long pedidoId) { this.pedidoId = pedidoId; }

    public String getUsuarioId() { return usuarioId; }
    public void setUsuarioId(String usuarioId) { this.usuarioId = usuarioId; }

    public String getCliente() { return cliente; }
    public void setCliente(String cliente) { this.cliente = cliente; }

    public String getLocalId() { return localId; }
    public void setLocalId(String localId) { this.localId = localId; }

    public String getLocalNombre() { return localNombre; }
    public void setLocalNombre(String localNombre) { this.localNombre = localNombre; }

    public String getCategoria() { return categoria; }
    public void setCategoria(String categoria) { this.categoria = categoria; }

    public String getProductos() { return productos; }
    public void setProductos(String productos) { this.productos = productos; }

    public Integer getCantidadTotal() { return cantidadTotal; }
    public void setCantidadTotal(Integer cantidadTotal) { this.cantidadTotal = cantidadTotal; }

    public Double getTotalPrecio() { return totalPrecio; }
    public void setTotalPrecio(Double totalPrecio) { this.totalPrecio = totalPrecio; }

    public String getEstado() { return estado; }
    public void setEstado(String estado) { this.estado = estado; }

    public String getFechaHora() { return fechaHora; }
    public void setFechaHora(String fechaHora) { this.fechaHora = fechaHora; }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public boolean isSimularErrorKafka() { return simularErrorKafka; }
    public void setSimularErrorKafka(boolean simularErrorKafka) { this.simularErrorKafka = simularErrorKafka; }
}
