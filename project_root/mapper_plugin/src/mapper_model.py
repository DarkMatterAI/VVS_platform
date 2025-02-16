# import torch
# import torch.nn as nn 

# class PassThroughLayer(nn.Module):
#     def __init__(self):
#         super().__init__()
        
#     def forward(self, x):
#         return x

# class LinearBlock(nn.Module):
#     def __init__(self, d_in, d_out, act=True, bn=False, dropout=0., **lin_kwargs):
#         super().__init__()

#         layers = [nn.Linear(d_in, d_out, **lin_kwargs)]
#         nn.init.kaiming_uniform_(layers[0].weight, mode='fan_in', nonlinearity='relu')

#         if bn:
#             layers.append(nn.BatchNorm1d(d_out))

#         if act:
#             layers.append(nn.ReLU())

#         if dropout>0.:
#             layers.append(nn.Dropout(p=dropout))

#         self.layers = nn.Sequential(*layers)

#     def forward(self, x):
#         return self.layers(x)

# class OutputLayer(nn.Module):
#     def __init__(self, d_hidden, d_out, n_out):
#         super().__init__()
        
#         self.layer = nn.Linear(d_hidden, d_out*n_out)
#         nn.init.kaiming_uniform_(self.layer.weight, mode='fan_in')
        
#         self.n_out = n_out
        
#     def forward(self, x):
#         x = self.layer(x)
        
#         if self.n_out>1:
#             x = torch.stack(torch.chunk(x, self.n_out, -1), 1)
            
#         return x

# class Mapper(nn.Module):
#     def __init__(self, input_layer, projector, output_layer):
#         super().__init__()
        
#         self.input_layer = self.check_layer(input_layer)
#         self.projector = self.check_layer(projector)
#         self.output_layer = self.check_layer(output_layer)
        
#     def check_layer(self, layer):
#         if layer is None:
#             layer = PassThroughLayer()
#         return layer
        
#     def forward(self, x):
#         x = self.input_layer(x)
#         x = self.projector(x)
#         x = self.output_layer(x)
#         return x

# class MLPProjector(nn.Module):
#     def __init__(self, d_hidden, n_layers, bn=False, dropout=0.):
#         super().__init__()
        
#         layers = [LinearBlock(d_hidden, d_hidden, act=True, bn=bn, dropout=dropout)
#                  for i in range(n_layers)]
#         self.layers = nn.Sequential(*layers)
        
#     def forward(self, x):
#         return self.layers(x)

# class MLPMapper(Mapper):
#     def __init__(self, d_in, n_in, d_hidden, n_layers, d_out, n_out, bn=False, dropout=0.):
#         input_layer = LinearBlock(d_in*n_in, d_hidden, act=True, bn=bn, dropout=dropout)
        
#         if n_layers > 0:
#             projector = MLPProjector(d_hidden, n_layers, bn=bn, dropout=dropout)
#         else:
#             projector = PassThroughLayer()
            
#         output_layer = OutputLayer(d_hidden, d_out, n_out)
#         super().__init__(input_layer, projector, output_layer)

