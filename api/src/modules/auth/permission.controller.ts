import { Controller, Get, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from './jwt-auth.guard';
import { PERMISSION_MODULES, ROLE_TEMPLATES } from './permission.config';

@UseGuards(JwtAuthGuard)
@Controller('permissions')
export class PermissionController {
  @Get('modules')
  listModules() {
    return PERMISSION_MODULES;
  }

  @Get('roles')
  listRoleTemplates() {
    return ROLE_TEMPLATES;
  }
}
